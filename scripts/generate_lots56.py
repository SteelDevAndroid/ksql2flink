"""Extract the eight lot-5/6 queries and generate their deployment contracts."""
import json
import re
from generate_conformite_sql import ROOT, SchemaParser


def main():
    log = (ROOT / 'kafka_streams_recovery.log').read_text()
    ddl = {}
    for m in re.finditer(r'^Statement\s*:\s*(.*?;)', log, re.M | re.S):
        q = m[1]
        ddl[re.search(r'CREATE (?:OR REPLACE )?(?:STREAM|TABLE) (\w+)', q)[1]] = q
    badge_queries = {}
    for m in re.finditer(r'^(IQ_HISTOBADGEVA_\w+) \(RUNNING\) : (INSERT INTO .*?;)', log, re.M):
        badge_queries.setdefault(m[1], m[2])
    if len(badge_queries) != 4:
        raise ValueError('Expected four badge INSERTs')
    reference, badge_sources, badge_outputs = [], {}, {}
    for direction, short in [('entree', 'BE'), ('sortie', 'VS')]:
        original = f'VA_PEAGE_VOIE_PASSAGE{direction.upper()}_CONFORME_ASF'
        name = original[:-3] + 'VA'
        topic = f'va.peage.backoffice.conformite.{direction}.conforme'
        declaration = ddl[original].replace(original, name).replace(f'asf.peage.backoffice.conformite.{direction}.conforme', topic)
        reference.append('-- Existing consolidated topic contract, identical to the ASF value schema.\n' + declaration)
        badge_sources[direction] = {'name': name, 'topic': topic}
        target = f'VA_PEAGE_VOIE_HISTOBADGEVA_{short}'
        reference.append(ddl[target])
        badge_outputs[direction] = {'name': target, 'topic': re.search(r"KAFKA_TOPIC='([^']+)'", ddl[target])[1]}
    reference.extend(badge_queries.values())
    (ROOT / 'sql/ksqldb/05-badges.sql').write_text('\n\n'.join(reference) + '\n')
    sql = ["""-- Four correlated badge queries; a single UNNEST per matching event, never a Cartesian product.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';
CREATE TEMPORARY SYSTEM FUNCTION JSON_ITEMS AS 'lab.JsonElements';
CREATE TEMPORARY SYSTEM FUNCTION JSON_PARTS AS 'lab.JsonObjectParts';
CREATE TEMPORARY SYSTEM FUNCTION JSON_STRING AS 'lab.JsonString';
CREATE TEMPORARY SYSTEM FUNCTION BADGE_HISTORY AS 'lab.BadgeHistory';
"""]
    for direction in ('entree', 'sortie'):
        sql.append(f"""CREATE TABLE badge_{direction}_input (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='{badge_sources[direction]['topic']}',
  'properties.bootstrap.servers'='kafka:9092', 'properties.group.id'='badge-{direction}',
  'scan.startup.mode'='earliest-offset', 'format'='raw');
CREATE VIEW badge_{direction} AS SELECT KSQL_PROJECT(payload, '{direction}') AS j, kafka_timestamp FROM badge_{direction}_input;
CREATE TABLE badge_{direction}_output (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='{badge_outputs[direction]['topic']}.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw', 'sink.delivery-guarantee'='at-least-once');
""")
    sql.append('EXECUTE STATEMENT SET\nBEGIN')
    for direction, short in [('entree', 'BE'), ('sortie', 'VS')]:
        passage = 'passageEntree' if direction == 'entree' else 'passageSortie'
        for flavor in ('CARDME', 'CIP'):
            query = badge_queries[f'IQ_HISTOBADGEVA_{short}_{flavor}']
            gate = query.split(' WHERE ', 1)[1].rsplit(' EMIT CHANGES', 1)[0]
            # Preserve every station literal and both station paths in the original WHERE.
            gate = re.sub(r'(`\w+`(?:->`\w+`)+)',
                          lambda m: "JSON_VALUE(j, '$." + m[0].replace('`', '').replace('->', '.') + "')", gate)
            pan_path = 'paymentMeans.PAN' if flavor == 'CARDME' else 'F_IDU'
            pan = f"JSON_VALUE(event_json, '$.{pan_path}')"
            # Exclude PAN sentinels only; the lambdas also contain the padding literal '0'.
            banned = sorted({x for x in re.findall(r"'(0+)'", query) if len(x) > 1})
            banned_sql = ','.join("'" + x + "'" for x in banned)
            names = ['DHM', 'sens', 'localisation']
            values = [f"JSON_STRING(JSON_VALUE(j, '$.{passage}.DHM'))", f"JSON_STRING('{ 'E' if direction == 'entree' else 'S' }')", f"JSON_QUERY(j, '$.{passage}.localisation')"]
            if direction == 'sortie':
                names += ['classe', 'tarif']
                values += ["JSON_STRING(JSON_VALUE(j,'$.passageSortie.tarification.classeTarifaire.classe'))",
                           "JSON_VALUE(j,'$.passageSortie.tarification.prix.montantTTC')"]
            names += ['PAN', 'histo', 'D_VET']
            values += [f'JSON_STRING({pan})', f"BADGE_HISTORY(event_json,'{flavor}')", "JSON_QUERY(event_json,'$.D_VET')"]
            fields = 'ARRAY[' + ','.join("'" + x + "'" for x in names) + ']'
            data = 'ARRAY[' + ','.join(values) + ']'
            sql.append(f"""INSERT INTO badge_{direction}_output
SELECT JSON_PARTS({fields}, {data}), kafka_timestamp
FROM badge_{direction} CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.evenements'))) AS e(event_json)
WHERE ({gate}) AND CHAR_LENGTH({pan})>0 AND {pan} NOT IN ({banned_sql})
  AND JSON_VALUE(event_json,'$.idEvenement')='DSRC.lecture.{flavor}';
""")
    sql.append('END;\n')
    (ROOT / 'sql/flink/05-badges.sql').write_text('\n'.join(sql))
    ref_sources = {'compteur_input': 'STREAM_VA_PEAGE_BACKOFFICE_SOCLEET_REFCOMPTEUR',
                   'indicateur_input': 'STREAM_VA_PEAGE_BACKOFFICE_SOCLEET_REFINDICATEUR',
                   'facts': 'VA_PEAGE_BACKOFFICE_SOCLEET_ETANCHEITE'}
    ref_targets = {'compteur_table': 'TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_COMPTEURS',
                   'indicateur_table': 'TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_INDICATEURS',
                   'compteur_output': 'VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR',
                   'indicateur_output': 'VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR'}
    reference, config, contracts = [], {'group_prefix': 'socleet-flink'}, {}
    for key, name in {**ref_sources, **ref_targets}.items():
        query = ddl[name]; reference.append(query)
        topic = re.search(r"KAFKA_TOPIC='([^']+)'", query)[1]
        config[key] = topic + ('.flink' if key in ref_targets else '')
        contracts[key] = {'name': name, 'topic': topic, 'source': key in ref_sources,
                          'cleanup': 'compact' if key.endswith('table') else 'delete'}
        if key in ref_sources:
            columns = query.split(' (', 1)[1].rsplit(') WITH', 1)[0]
            parser = SchemaParser(columns); schema = parser.fields()
            if parser.index != len(parser.tokens): raise ValueError('Unconsumed lot-6 schema')
            resource_name = 'lot6-' + key.replace('_input', '')
            (ROOT / f'docker/flink/resources/schemas/{resource_name}.json').write_text(json.dumps(schema, indent=2) + '\n')
    (ROOT / 'sql/ksqldb/06-socleet.sql').write_text('\n\n'.join(reference) + '\n')
    (ROOT / 'sql/flink/06-socleet.json').write_text(json.dumps(config, indent=2) + '\n')
    manifest = {'badge_sources': badge_sources, 'badge_outputs': badge_outputs, 'socleet': contracts}
    (ROOT / 'docs/lots56-contracts.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
