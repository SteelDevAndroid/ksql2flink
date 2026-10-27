"""Extract the five stateful window queries and existing Kafka input contracts."""
import json
import re
from generate_conformite_sql import ROOT, SchemaParser


def main():
    log = (ROOT / 'kafka_streams_recovery.log').read_text(encoding='utf-8')
    ddl = {}
    for m in re.finditer(r'^Statement\s*:\s*(.*?;)', log, re.M | re.S):
        ddl[re.search(r'CREATE (?:OR REPLACE )?(?:STREAM|TABLE) (\w+)', m[1])[1]] = '\n'.join(
            line.rstrip() for line in m[1].splitlines())
    sources = {
        'indicator_input': 'VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_INDICATEUR',
        'flows': 'VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND',
    }
    targets = {
        'hourly': 'TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_INDICATEUR',
        'quality': 'VA_PEAGE_CLASSIFICATIONINFOQUALITE',
        'count': 'VA_PEAGE_CLASSIFICATIONINFOQUALITECOUNT',
        'unit_alarm': 'VA_PEAGE_CLASSIFICATION_ALARMEUNITAIRE',
        'global_alarm': 'VA_PEAGE_CLASSIFICATION_ALARMEGLOBALE',
    }
    # These are existing upstream topic contracts, not replacement lot-3/6 queries.
    indicator = json.loads((ROOT / 'docker/flink/resources/schemas/lot34-compteur-input.json').read_text())
    for field in ['MONTANT_TRANSACTION', 'C_R_MODE_REGLEMENTID', 'R_COMPTEUR_TYPEID']:
        indicator.pop(field)
    indicator['R_INDICATEUR_TYPEID'] = 'INTEGER'
    flows = json.loads((ROOT / 'docker/flink/resources/schemas/lot34-flows.json').read_text())
    flows['STATIONLANE'] = 'STRING'
    schemas = {'indicator_input': indicator, 'flows': flows}
    reference, manifest, config = [], {}, {'group_prefix': 'windows-flink'}
    for key, name in sources.items():
        schema = schemas[key]
        if key == 'flows':
            base = ddl['VA_PEAGE_CLASSIFICATION_FLOWS']
            columns = base.split(' (', 1)[1].rsplit(') WITH', 1)[0] + ', STATIONLANE STRING'
            topic = re.search(r"KAFKA_TOPIC='([^']+)'", ddl[name])[1]
        else:
            columns = 'R_COMPTEURID INTEGER KEY, ' + ', '.join(f'{k} {v}' for k, v in schema.items())
            topic = name
        reference.append(f"CREATE STREAM {name} ({columns}) WITH (KAFKA_TOPIC='{topic}', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');")
        manifest[key] = {'name': name, 'topic': topic, 'source': True}
        config[key] = topic
        (ROOT / f'docker/flink/resources/schemas/lot78-{key.replace("_", "-")}.json').write_text(json.dumps(schema, indent=2) + '\n', encoding='utf-8')
    for key, name in targets.items():
        if key == 'count':
            reference.append(ddl['VA_PEAGE_CLASSIFICATIONINFOQUALITE_STREAM'])
        if key == 'unit_alarm':
            reference.append(ddl['VA_PEAGE_CLASSIFICATIONINFOQUALITE_COUNT_STREAM'])
        reference.append(ddl[name])
        topic = re.search(r"KAFKA_TOPIC='([^']+)'", ddl[name])[1]
        manifest[key] = {'name': name, 'topic': topic, 'source': False}
        config[key] = topic + '.flink'
    for kind, name in [('quality', 'VA_PEAGE_CLASSIFICATIONINFOQUALITE_STREAM'),
                       ('count', 'VA_PEAGE_CLASSIFICATIONINFOQUALITE_COUNT_STREAM')]:
        columns = ddl[name].split(' (', 1)[1].rsplit(') WITH', 1)[0]
        # Window bounds are TIMESTAMP in the re-read contract but neither join nor count
        # references them. Only parse the columns consumed by this job.
        columns = re.sub(r', WINDOW_\w+ TIMESTAMP', '', columns)
        schema = SchemaParser(columns).fields()
        (ROOT / f'docker/flink/resources/schemas/lot78-{kind}-input.json').write_text(json.dumps(schema, indent=2) + '\n', encoding='utf-8')
    (ROOT / 'sql/ksqldb/07-08-windows.sql').write_text('\n\n'.join(reference) + '\n', encoding='utf-8')
    for stage, indexes in {'hourly': [0, 2], 'quality': [1, 3], 'count': [4, 5], 'alarms': [4, 6, 7, 8]}.items():
        (ROOT / f'sql/ksqldb/07-08-{stage}.sql').write_text('\n\n'.join(reference[i] for i in indexes) + '\n', encoding='utf-8')
    (ROOT / 'sql/flink/07-08-windows.json').write_text(json.dumps(config, indent=2) + '\n', encoding='utf-8')
    (ROOT / 'docs/lots78-contracts.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
