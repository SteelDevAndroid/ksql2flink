"""Shared isolation and transport for ordered differential migration scenarios."""
import json
import re
import time
import uuid
from smoke import ROOT, docker, request, canonical
from test_lexhistoric import probe


class Pipeline:
    def __init__(self, label):
        self.run_id = label + uuid.uuid4().hex[:8]
        self.artifact = ROOT / 'artifacts' / self.run_id
        self.artifact.mkdir(parents=True)
        self.queries = []
        self.job = None

    def topic(self, name, compact=False):
        topic = self.run_id + '.' + name
        docker('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9092',
               '--create', '--topic', topic, '--partitions', '6', '--replication-factor', '1',
               '--config', 'cleanup.policy=' + ('compact' if compact else 'delete'))
        return topic

    def reference(self, path, topics, count):
        sql = (ROOT / path).read_text(encoding='utf-8')
        for old, new in topics.items():
            sql = sql.replace("'" + old + "'", "'" + new + "'")
        names = re.findall(r'CREATE (?:OR REPLACE )?(?:STREAM|TABLE) (\w+)', sql)
        names += re.findall(r"QUERY_ID\s*=\s*'(\w+)'", sql)
        for name in sorted(set(names), key=len, reverse=True):
            sql = re.sub(r'\b' + name + r'\b', self.run_id.upper() + '_' + name, sql)
        sql = re.sub(r'REPLICAS=\d+', 'REPLICAS=1', sql)
        sql = re.sub(r"CLEANUP_POLICY='[^']*',\s*", '', sql)
        # DESCRIBE also prints retention for non-windowed tables, which the submit API rejects.
        sql = re.sub(r'CREATE (?:OR REPLACE )?TABLE .*?;',
                     lambda m: m[0] if 'WINDOW ' in m[0] else re.sub(
                         r'(?:RETENTION_MS=\d+,\s*|,\s*RETENTION_MS=\d+)', '', m[0]), sql, flags=re.S)
        (self.artifact / 'ksql.sql').write_text(sql, encoding='utf-8')
        request('http://localhost:8088/ksql', {'ksql': sql, 'streamsProperties': {'auto.offset.reset': 'earliest'}})
        result = request('http://localhost:8088/ksql', {'ksql': 'SHOW QUERIES;', 'streamsProperties': {}})
        self.queries = [q for item in result for q in item.get('queries', [])
                        if self.run_id.upper() in q.get('queryString', '')]
        if len(self.queries) != count:
            raise AssertionError(f'Expected {count} reference queries, found {len(self.queries)}')

    def launch(self, path, config=None, topics=None, main_class='lab.SocleEtJob'):
        if config is None:
            sql = (ROOT / path).read_text()
            for old, new in (topics or {}).items():
                sql = sql.replace("'" + old + "'", "'" + new + "'")
            file = self.artifact / 'flink.sql'
            file.write_text(sql)
            docker('cp', str(file), 'jobmanager:/tmp/pipeline.sql')
            output = docker('exec', '-T', 'jobmanager', '/opt/flink/bin/sql-client.sh', 'embedded', '-f', '/tmp/pipeline.sql')
        else:
            file = self.artifact / 'flink-config.json'
            file.write_text(json.dumps(config, indent=2))
            docker('cp', str(file), 'jobmanager:/tmp/pipeline-config.json')
            output = docker('exec', '-T', 'jobmanager', '/opt/flink/bin/flink', 'run', '-d', '-c', main_class,
                            '/opt/flink/lib/migration-lab.jar', '--config', '/tmp/pipeline-config.json')
        (self.artifact / 'flink-submit.log').write_text(output, encoding='utf-8')
        match = re.search(r'(?:Job ID:|JobID)\s*([0-9a-f]{32})', output)
        if not match or '[ERROR]' in output:
            raise RuntimeError(f'Submission failed; see {self.artifact}:\n{output[-2000:]}')
        self.job = match[1]
        deadline = time.monotonic() + 90
        while True:
            state = request('http://localhost:8081/jobs/' + self.job)['state']
            if state == 'RUNNING':
                break
            if state in ('FAILED', 'CANCELED', 'FINISHED') or time.monotonic() > deadline:
                raise RuntimeError('Flink job state: ' + state)
            time.sleep(1)

    def produce(self, records, phase):
        file = self.artifact / (phase + '.jsonl')
        file.write_text('\n'.join(json.dumps(row, ensure_ascii=False) for row in records), encoding='utf-8')
        docker('cp', str(file), 'jobmanager:/tmp/pipeline-input.jsonl')
        probe('produce-many', 'unused', '/tmp/pipeline-input.jsonl')

    def barrier(self, specs, phase):
        file = self.artifact / (phase + '-barrier.json')
        file.write_text(json.dumps(specs, indent=2))
        docker('cp', str(file), 'jobmanager:/tmp/pipeline-barrier.json')
        deadline = time.monotonic() + 120
        stable = 0
        observations = []
        while time.monotonic() < deadline:
            status = probe('barriers', '/tmp/pipeline-barrier.json')[0]
            observations.append(status)
            stable = stable + 1 if all(item['ready'] for item in status.values()) else 0
            if stable >= 2:
                (self.artifact / (phase + '-offsets.json')).write_text(json.dumps(observations, indent=2))
                return
            time.sleep(1)
        raise AssertionError(f'Offset barrier timed out: {status}')

    def compare(self, outputs, expected):
        actual = {name: {} for name in outputs}
        for engine in ('ksql', 'flink'):
            mapping = {topics[engine]: name for name, topics in outputs.items()}
            grouped = {name: [] for name in outputs}
            for row in probe('consume-many', ','.join(mapping)):
                name = mapping[row.pop('topic')]
                grouped[name].append(row)
            for name, rows in grouped.items():
                actual[name][engine] = rows
        report = {'run_id': self.run_id, 'expected': expected, 'actual': actual}
        (self.artifact / 'comparison.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        errors = []
        for name, engines in actual.items():
            for engine, rows in engines.items():
                missing = canonical(expected[name]) - canonical(rows)
                extra = canonical(rows) - canonical(expected[name])
                if missing or extra:
                    errors.append(f'{name}/{engine}: missing={sum(missing.values())}, extra={sum(extra.values())}')
        if errors:
            raise AssertionError('; '.join(errors) + f'; report={self.artifact}')
        return sum(map(len, expected.values()))

    def close(self):
        if self.job:
            request('http://localhost:8081/jobs/' + self.job + '?mode=cancel', method='PATCH')
        for query in self.queries:
            request('http://localhost:8088/ksql', {'ksql': f'TERMINATE {query["id"]};', 'streamsProperties': {}})
