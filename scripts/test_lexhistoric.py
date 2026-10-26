"""Run all five real Lexhistoric queries against Kafka, ksqlDB and Flink."""
import base64
import json
import re
import time
import uuid
from smoke import ROOT, docker, request, canonical

NETWORKS = ('ASF', 'ESC', 'ARS', 'ACR', 'COF')


def probe(*args):
    output = docker('exec', '-T', 'jobmanager', 'java', '-cp', '/opt/flink/lib/*',
                    'lab.KafkaProbe', *args, timeout=100)
    return [json.loads(line) for line in output.splitlines() if line.startswith('{')]


def main():
    run_id = 'lex' + uuid.uuid4().hex[:8]
    artifact = ROOT / 'artifacts' / run_id
    artifact.mkdir(parents=True)
    fixtures = json.loads((ROOT / 'fixtures/lexhistoric.json').read_text(encoding='utf-8'))
    original_output = 'va.peage.voie.passagesortieva.lexhistoric'
    output_ksql = f'{run_id}.output.ksql'
    output_flink = f'{run_id}.output.flink'
    inputs = {net: f'{run_id}.{net.lower()}.input' for net in NETWORKS}
    for topic, partitions in [(t, 6) for t in inputs.values()] + [(output_ksql, 2), (output_flink, 2)]:
        docker('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9092',
               '--create', '--topic', topic, '--partitions', str(partitions), '--replication-factor', '1',
               '--config', 'cleanup.policy=delete', '--config', 'retention.ms=86400000')

    ksql = (ROOT / 'sql/ksqldb/02-lexhistoric.sql').read_text()
    flink = (ROOT / 'sql/flink/02-lexhistoric.sql').read_text()
    ksql = ksql.replace(original_output, output_ksql)
    flink = flink.replace(original_output + '.flink', output_flink)
    for net, topic in inputs.items():
        original_input = f'{net.lower()}.peage.voie.passagesortieva'
        ksql = ksql.replace(original_input, topic)
        flink = flink.replace(original_input, topic)
        for suffix in ('PEAGE_VOIE_PASSAGESORTIEVA_STREAM', 'PEAGE_LEXVA_PASSAGESORTIEVA_LEXHISTORIC_STREAM'):
            ksql = ksql.replace(f'{net}_{suffix}', f'{run_id.upper()}_{net}_{suffix}')
    ksql = re.sub(r'REPLICAS=\d+', 'REPLICAS=1', ksql)
    # DESCRIBE prints this inferred property, but ksqlDB rejects it in submitted DDL.
    ksql = ksql.replace("CLEANUP_POLICY='delete', ", '')
    query_ids, job_id = [], None
    try:
        # Read the journal queries as the oracle; only deployment names/replication change.
        request('http://localhost:8088/ksql', {'ksql': ksql, 'streamsProperties': {'auto.offset.reset': 'earliest'}})
        queries = request('http://localhost:8088/ksql', {'ksql': 'SHOW QUERIES;', 'streamsProperties': {}})
        query_ids = [q['id'] for item in queries for q in item.get('queries', [])
                     if run_id.upper() in q.get('queryString', '')]
        assert len(query_ids) == 5, query_ids
        sql_file = artifact / 'flink.sql'
        sql_file.write_text(flink, encoding='utf-8')
        docker('cp', str(sql_file), 'jobmanager:/tmp/lexhistoric.sql')
        output = docker('exec', '-T', 'jobmanager', '/opt/flink/bin/sql-client.sh',
                        'embedded', '-f', '/tmp/lexhistoric.sql')
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
        expected = []
        for network_index, (net, topic) in enumerate(inputs.items()):
            records = []
            for index, case in enumerate(fixtures):
                key = None if case.get('null_key') else base64.b64encode(f'{net}-{case["name"]}'.encode()).decode()
                timestamp = 1792922400000 + network_index * 100000 + index * 1000
                records.append({'key': key, 'timestamp': timestamp, 'partition': index % 6, 'value': case['value']})
                if case['expected'] is not None:
                    # No KEY column in the original DDL: ksqlDB emits a null output key.
                    expected.append({'key': None, 'timestamp': timestamp, 'value': case['expected']})
            data_file = artifact / f'{net}.jsonl'
            data_file.write_text('\n'.join(json.dumps(r, ensure_ascii=False) for r in records), encoding='utf-8')
            docker('cp', str(data_file), 'jobmanager:/tmp/lexhistoric-input.jsonl')
            probe('produce', topic, '/tmp/lexhistoric-input.jsonl')
        actual = {'ksql': probe('consume', output_ksql), 'flink': probe('consume', output_flink)}
        report = {'run_id': run_id, 'input_count': len(fixtures) * 5, 'expected': expected, 'actual': actual}
        (artifact / 'comparison.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        for engine, rows in actual.items():
            if canonical(rows) != canonical(expected):
                missing = canonical(expected) - canonical(rows)
                extra = canonical(rows) - canonical(expected)
                raise AssertionError(f'{engine}: {len(rows)} outputs; missing={sum(missing.values())}; '
                                     f'extra={sum(extra.values())}; report={artifact}')
        print(f'PASS: {len(fixtures)*5} inputs / {len(expected)} outputs per engine; keys, timestamps, values and multiplicity. {artifact}')
    finally:
        if job_id:
            request('http://localhost:8081/jobs/' + job_id + '?mode=cancel', method='PATCH')
        for query_id in query_ids:
            request('http://localhost:8088/ksql', {'ksql': f'TERMINATE {query_id};', 'streamsProperties': {}})


if __name__ == '__main__':
    main()
