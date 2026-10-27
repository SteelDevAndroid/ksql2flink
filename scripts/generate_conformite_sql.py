"""Extract lot 2 and its exact serialization schemas from the recovery journal."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SchemaParser:
    def __init__(self, text):
        self.tokens = re.findall(r'`[^`]+`|[A-Za-z_][\w-]*|[<>,]', text)
        self.index = 0

    def take(self):
        token = self.tokens[self.index]
        self.index += 1
        return token

    def expect(self, token):
        actual = self.take()
        if actual != token:
            raise ValueError(f'Expected {token}, got {actual}')

    def fields(self):
        result = {}
        while self.index < len(self.tokens) and self.tokens[self.index] != '>':
            name = self.take()
            if name.startswith('`') and name[1:-1].upper() == name[1:-1]:
                raise ValueError('Quoted uppercase field requires explicit matching metadata: ' + name)
            name = name[1:-1] if name.startswith('`') else name.upper()
            result[name] = self.type()
            if self.index == len(self.tokens) or self.tokens[self.index] == '>':
                break
            self.expect(',')
        return result

    def type(self):
        token = self.take()
        if token == 'STRUCT':
            self.expect('<')
            result = self.fields()
            self.expect('>')
            return result
        if token == 'ARRAY':
            self.expect('<')
            result = [self.type()]
            self.expect('>')
            return result
        if token not in ('STRING', 'INTEGER', 'DOUBLE'):
            raise ValueError('Unsupported source type: ' + token)
        return token


def main():
    log = (ROOT / 'kafka_streams_recovery.log').read_text()
    declarations = [m[1] for m in re.finditer(r'^Statement\s*:\s*(.*?;)', log, re.M | re.S)]
    reference, inserts, inputs = [], {}, {}
    for direction, networks in [('entree', ['ASF', 'ESC', 'ACR', 'COF']),
                                ('sortie', ['ASF', 'ESC', 'ARS', 'ACR', 'COF'])]:
        schemas = []
        for network in networks:
            name = f'VA_PEAGE_VOIE_PASSAGE{direction.upper()}_CONFORME_{network}'
            ddl = next(q for q in declarations if q.startswith(f'CREATE STREAM {name} ('))
            reference.append(ddl)
            columns = ddl.split(' (', 1)[1].rsplit(') WITH', 1)[0]
            parser = SchemaParser(columns)
            schemas.append(parser.fields())
            if parser.index != len(parser.tokens):
                raise ValueError('Unconsumed schema tokens')
            inputs[(direction, network)] = re.search(r"KAFKA_TOPIC='([^']+)'", ddl)[1]
        if any(schema != schemas[0] for schema in schemas):
            raise ValueError('Network schema mismatch: ' + direction)
        resource = ROOT / f'docker/flink/resources/schemas/conformite-{direction}.json'
        resource.parent.mkdir(parents=True, exist_ok=True)
        resource.write_text(json.dumps(schemas[0], indent=2) + '\n', encoding='utf-8')
        name = f'VA_PEAGE_VOIE_PASSAGE{direction.upper()}_CONFORME_VA'
        reference.append(next(q for q in declarations if q.startswith(f'CREATE STREAM {name} WITH')))
    for m in re.finditer(r'^(IQ_\w+) \(RUNNING\) : (INSERT INTO .*?;)', log, re.M):
        if 'CONFORME_VA_FROM_' in m[1]:
            inserts.setdefault(m[1], m[2])
    if len(inserts) != 7:
        raise ValueError('Expected seven consolidation INSERTs')
    reference.extend(inserts.values())
    (ROOT / 'sql/ksqldb/03-conformite.sql').write_text(
        '-- Journal reference, unchanged except deployment adaptation by the test runner.\n' +
        '\n\n'.join(reference) + '\n', encoding='utf-8')
    sql = ["""-- Lot 2: nine append-only consolidation branches; no ARS entree branch in the journal.
-- Branches of one direction share schema and sink: one multi-topic source per direction
-- replaces the per-network sources and UNION ALL. Per-partition order is unchanged.
-- ProjectKsqlJson reproduces SELECT * schema serialization, not a raw JSON passthrough.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';
"""]
    for direction in ('entree', 'sortie'):
        topics = ';'.join(t for (d, _), t in inputs.items() if d == direction)
        sql.append(f"""CREATE TABLE {direction}_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='{topics}',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='conformite-flink-{direction}',
  'scan.startup.mode'='earliest-offset', 'format'='raw'
);

CREATE TABLE {direction}_output (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka', 'topic'='va.peage.backoffice.conformite.{direction}.conforme.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw',
  'sink.delivery-guarantee'='at-least-once'
);
""")
    sql.append('EXECUTE STATEMENT SET\nBEGIN')
    for direction in ('entree', 'sortie'):
        sql.append(f"INSERT INTO {direction}_output\n"
                   f"SELECT KSQL_PROJECT(payload, '{direction}'), kafka_timestamp FROM {direction}_input;")
    sql.append('END;\n')
    (ROOT / 'sql/flink/03-conformite.sql').write_text('\n'.join(sql), encoding='utf-8')


if __name__ == '__main__':
    main()
