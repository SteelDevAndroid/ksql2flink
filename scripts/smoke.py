"""Compare a ksqlDB/Flink projection against an independent expected result."""
import collections
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.request
import urllib.error
import uuid

from flink_kafka import resolve

ROOT = Path(__file__).resolve().parents[1]


def docker(*args, data=None, timeout=180):
    result = subprocess.run(['docker', 'compose', *args], cwd=ROOT, input=data,
                            text=True, encoding='utf-8', capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def request(url, payload=None, method=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, body, {'Content-Type': 'application/json'}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'{url}: {error.code}: {error.read().decode()}') from error


def consume(topic):
    result = subprocess.run(
        ['docker', 'compose', 'exec', '-T', 'kafka', 'kafka-console-consumer',
         '--bootstrap-server', 'kafka:9092', '--topic', topic,
         '--from-beginning', '--timeout-ms', '10000', '--max-messages', '5'],
        cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=45)
    if result.returncode and 'TimeoutException' not in result.stderr:
        raise RuntimeError(result.stdout + result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def canonical(rows):
    return collections.Counter(json.dumps(row, sort_keys=True) for row in rows)


def main():
    run_id = uuid.uuid4().hex[:10]
    artifact = ROOT / 'artifacts' / run_id
    artifact.mkdir(parents=True)
    topics = {name: f'lab.{run_id}.{name}' for name in ('input', 'output.ksql', 'output.flink')}
    for topic in topics.values():
        docker('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9092',
               '--create', '--topic', topic, '--partitions', '6', '--replication-factor', '1')

    def sql(engine):
        content = (ROOT / 'sql' / engine / '01-smoke.sql').read_text()
        for old, new in topics.items():
            content = content.replace('lab.' + old, new)
        if engine == 'ksqldb':
            content = content.replace('LAB_INPUT', 'LAB_INPUT_' + run_id.upper())
            content = content.replace('LAB_KSQL', 'LAB_KSQL_' + run_id.upper())
            return content
        return resolve(content)

    query_ids = []
    job_id = None
    try:
        request('http://localhost:8088/ksql', {'ksql': sql('ksqldb'), 'streamsProperties': {}})
        statements = request('http://localhost:8088/ksql', {'ksql': 'SHOW QUERIES;', 'streamsProperties': {}})
        for statement in statements:
            query_ids.extend(q['id'] for q in statement.get('queries', [])
                             if ('LAB_KSQL_' + run_id.upper()) in q.get('queryString', ''))
        if not query_ids:
            raise RuntimeError('Could not identify the submitted ksqlDB query')
        flink_sql = artifact / 'flink.sql'
        flink_sql.write_text(sql('flink'), encoding='utf-8')
        docker('cp', str(flink_sql), 'jobmanager:/tmp/lab-smoke.sql')
        output = docker('exec', '-T', 'jobmanager', '/opt/flink/bin/sql-client.sh',
                        'embedded', '-f', '/tmp/lab-smoke.sql')
        (artifact / 'flink-submit.log').write_text(output, encoding='utf-8')
        match = re.search(r'Job ID:\s*([0-9a-f]{32})', output)
        if not match or '[ERROR]' in output:
            raise RuntimeError('Flink SQL submission failed:\n' + output)
        job_id = match.group(1)
        deadline = time.monotonic() + 90
        while True:
            state = request('http://localhost:8081/jobs/' + job_id)['state']
            if state == 'RUNNING':
                break
            if state in ('FAILED', 'CANCELED', 'FINISHED') or time.monotonic() > deadline:
                raise RuntimeError('Flink job did not reach RUNNING: ' + state)
            time.sleep(1)
        docker('exec', '-T', 'kafka', 'kafka-console-producer', '--bootstrap-server',
               'kafka:9092', '--topic', topics['input'],
               data=(ROOT / 'fixtures' / 'smoke.jsonl').read_text())
        expected = [
            {'ID': 'a', 'AMOUNT': 1.5, 'MODEREGLEMENT': 256},
            {'ID': 'b', 'AMOUNT': 2.0, 'MODEREGLEMENT': 512},
            {'ID': 'c', 'AMOUNT': 3.0, 'MODEREGLEMENT': 1024},
            {'ID': 'd', 'AMOUNT': 4.0, 'MODEREGLEMENT': 2048},
        ]
        results = {engine: consume(topics['output.' + engine]) for engine in ('ksql', 'flink')}
        report = {'run_id': run_id, 'topics': topics, 'expected': expected, 'actual': results}
        (artifact / 'comparison.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        for engine, rows in results.items():
            if canonical(rows) != canonical(expected):
                raise AssertionError(f'{engine}: unexpected output; see {artifact}')
        print(f'PASS: 6 inputs, 4 expected outputs on each engine. Report: {artifact}')
    finally:
        if job_id:
            request('http://localhost:8081/jobs/' + job_id + '?mode=cancel', method='PATCH')
        for query_id in query_ids:
            request('http://localhost:8088/ksql', {'ksql': f'TERMINATE {query_id};', 'streamsProperties': {}})


if __name__ == '__main__':
    main()
