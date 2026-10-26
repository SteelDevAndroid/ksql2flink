"""Compare all nine consolidation branches with the journal's original queries."""
import base64
import copy
import json
import re
import time
import uuid
from smoke import ROOT, docker, request, canonical
from test_lexhistoric import probe

NETWORKS = ('ASF', 'ESC', 'ARS', 'ACR', 'COF')


def change_names(value, only_unquoted=False):
    if isinstance(value, dict):
        return {(k.lower() if only_unquoted and k == k.upper() else
                 k if only_unquoted else k.swapcase()): change_names(v, only_unquoted)
                for k, v in value.items()}
    if isinstance(value, list):
        return [change_names(v, only_unquoted) for v in value]
    return value


def load_fixtures():
    data = json.loads((ROOT / 'fixtures/conformite.json').read_text(encoding='utf-8'))
    fixtures = {}
    for direction, spec in data.items():
        cases = []
        for raw in spec['cases']:
            case = copy.deepcopy(raw)
            if case.get('expected_full'):
                case['expected'] = copy.deepcopy(spec['full_record'])
            variant = case.get('input_variant')
            if variant:
                value = copy.deepcopy(spec['full_record'])
                if variant == 'mixed-case':
                    value = change_names(value)
                elif variant == 'unquoted-field-casing':
                    value = change_names(value, only_unquoted=True)
                elif variant == 'unknown-fields':
                    value['unknownRoot'] = 'DROP'
                    value['message']['unknownNested'] = 'DROP'
                    value['evenements'][0]['unknownEvent'] = 'DROP'
                case['value'] = value
            cases.append(case)
        fixtures[direction] = cases
    return fixtures


def main():
    run_id = 'conf' + uuid.uuid4().hex[:8]
    artifact = ROOT / 'artifacts' / run_id
    artifact.mkdir(parents=True)
    fixtures = load_fixtures()
    ksql = (ROOT / 'sql/ksqldb/03-conformite.sql').read_text()
    flink = (ROOT / 'sql/flink/03-conformite.sql').read_text()
    outputs, inputs = {}, {}
    for direction in ('entree', 'sortie'):
        outputs[direction] = {engine: f'{run_id}.{direction}.{engine}' for engine in ('ksql', 'flink')}
        topic = f'va.peage.backoffice.conformite.{direction}.conforme'
        ksql = ksql.replace(topic, outputs[direction]['ksql'])
        flink = flink.replace(topic + '.flink', outputs[direction]['flink'])
        for network in NETWORKS:
            topic = f'{network.lower()}.peage.backoffice.conformite.{direction}.conforme'
            inputs[(direction, network)] = f'{run_id}.{direction}.{network.lower()}'
            ksql = ksql.replace(topic, inputs[(direction, network)])
            flink = flink.replace(topic, inputs[(direction, network)])
    # Isolate every ksqlDB object and explicit query ID, including INSERT INTO statements.
    ksql = re.sub(r'\bVA_PEAGE_VOIE_\w+', lambda m: run_id.upper() + '_' + m[0], ksql)
    ksql = re.sub(r'\bIQ_\w+', lambda m: run_id.upper() + '_' + m[0], ksql)
    ksql = re.sub(r'REPLICAS=\d+', 'REPLICAS=1', ksql).replace("CLEANUP_POLICY='delete', ", '')
    topics = list(inputs.values()) + [t for engines in outputs.values() for t in engines.values()]
    for topic in topics:
        docker('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9092',
               '--create', '--topic', topic, '--partitions', '6', '--replication-factor', '1',
               '--config', 'retention.ms=604800000', '--config', 'cleanup.policy=delete')
    query_ids, job_id = [], None
    try:
        request('http://localhost:8088/ksql', {'ksql': ksql, 'streamsProperties': {'auto.offset.reset': 'earliest'}})
        queries = request('http://localhost:8088/ksql', {'ksql': 'SHOW QUERIES;', 'streamsProperties': {}})
        query_ids = [q['id'] for item in queries for q in item.get('queries', [])
                     if run_id.upper() in q.get('queryString', '')]
        if len(query_ids) != 9:
            raise AssertionError(f'Expected nine journal queries, found {query_ids}')
        (artifact / 'ksql.sql').write_text(ksql, encoding='utf-8')
        sql_file = artifact / 'flink.sql'
        sql_file.write_text(flink, encoding='utf-8')
        docker('cp', str(sql_file), 'jobmanager:/tmp/conformite.sql')
        output = docker('exec', '-T', 'jobmanager', '/opt/flink/bin/sql-client.sh',
                        'embedded', '-f', '/tmp/conformite.sql')
        (artifact / 'flink-submit.log').write_text(output, encoding='utf-8')
        match = re.search(r'Job ID:\s*([0-9a-f]{32})', output)
        if not match or '[ERROR]' in output:
            raise RuntimeError(output)
        job_id = match[1]
        deadline = time.monotonic() + 90
        while True:
            state = request('http://localhost:8081/jobs/' + job_id)['state']
            if state == 'RUNNING':
                break
            if state in ('FAILED', 'CANCELED', 'FINISHED') or time.monotonic() > deadline:
                raise RuntimeError('Flink job state: ' + state)
            time.sleep(1)
        expected = {'entree': [], 'sortie': []}
        for source_index, ((direction, network), topic) in enumerate(inputs.items()):
            records = []
            for index, case in enumerate(fixtures[direction]):
                timestamp = 1792922400000 + source_index * 100000 + index * 1000
                key = base64.b64encode(f'{network}-{direction}-{index}'.encode()).decode() if index % 2 else None
                partition = index % 6
                if 'duplicate_of' in case:
                    original = records[case['duplicate_of']]
                    timestamp, key, partition = original['timestamp'], original['key'], original['partition']
                records.append({'key': key, 'timestamp': timestamp, 'partition': partition, 'value': case['value']})
                if (direction, network) != ('entree', 'ARS'):
                    expected[direction].append({'key': None, 'timestamp': timestamp, 'value': case['expected']})
            data_file = artifact / f'{direction}-{network}.jsonl'
            data_file.write_text('\n'.join(json.dumps(r, ensure_ascii=False) for r in records), encoding='utf-8')
            docker('cp', str(data_file), 'jobmanager:/tmp/conformite-input.jsonl')
            probe('produce', topic, '/tmp/conformite-input.jsonl')
        actual = {direction: {engine: probe('consume', topic) for engine, topic in engines.items()}
                  for direction, engines in outputs.items()}
        report = {'run_id': run_id, 'input_count': sum(len(fixtures[d]) for d, _ in inputs),
                  'expected': expected, 'actual': actual}
        (artifact / 'comparison.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        for direction, engines in actual.items():
            for engine, rows in engines.items():
                missing = canonical(expected[direction]) - canonical(rows)
                extra = canonical(rows) - canonical(expected[direction])
                if missing or extra:
                    raise AssertionError(f'{direction}/{engine}: missing={sum(missing.values())}, '
                                         f'extra={sum(extra.values())}; report={artifact}')
        print(f'PASS: {report["input_count"]} inputs, {len(expected["entree"])} entree / '
              f'{len(expected["sortie"])} sortie outputs per engine; '
              f'values, null keys, timestamps, exact duplicates and ARS exclusion. {artifact}')
    finally:
        if job_id:
            request('http://localhost:8081/jobs/' + job_id + '?mode=cancel', method='PATCH')
        for query_id in query_ids:
            request('http://localhost:8088/ksql', {'ksql': f'TERMINATE {query_id};', 'streamsProperties': {}})


if __name__ == '__main__':
    main()
