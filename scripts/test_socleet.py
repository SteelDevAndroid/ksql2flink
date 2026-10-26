"""Ordered reference/fact scenarios, with committed-offset barriers on both engines."""
import base64
import json
import struct
from lab_pipeline import Pipeline
from smoke import ROOT


def integer_key(value):
    return base64.b64encode(struct.pack('>i', value)).decode()


def main():
    lab = Pipeline('socle')
    manifest = json.loads((ROOT / 'docs/lots56-contracts.json').read_text())['socleet']
    fixture = json.loads((ROOT / 'fixtures/socleet.json').read_text())
    input_keys = [k for k, v in manifest.items() if v['source']]
    output_keys = [k for k, v in manifest.items() if not v['source']]
    inputs = {k: lab.topic(k) for k in input_keys}
    outputs = {k: {e: lab.topic(k + '.' + e, compact=k.endswith('table'))
                   for e in ('ksql', 'flink')} for k in output_keys}
    topics = {info['topic']: inputs[k] if info['source'] else outputs[k]['ksql']
              for k, info in manifest.items()}
    config = dict(inputs, group_prefix=lab.run_id)
    config.update({k: engines['flink'] for k, engines in outputs.items()})
    expected = {k: [] for k in outputs}
    origin = 1792922400000
    try:
        lab.reference('sql/ksqldb/06-socleet.sql', topics, 4)
        lab.launch(None, config=config)
        query_ids = {}
        for key in output_keys:
            target = lab.run_id.upper() + '_' + manifest[key]['name']
            query_ids[key] = next(q['id'] for q in lab.queries
                                 if q['queryString'].startswith('CREATE TABLE ' + target + ' ')
                                 or q['queryString'].startswith('CREATE STREAM ' + target + ' '))

        def barrier(kind, phase):
            specs = {}
            if kind == 'facts':
                specs['flink-facts'] = {'group': lab.run_id + '-facts', 'topic': inputs['facts']}
                for key in ['compteur_output', 'indicateur_output']:
                    specs['ksql-' + key] = {'contains': query_ids[key], 'topic': inputs['facts']}
            elif kind == 'aggregate':
                for name in ('compteur', 'indicateur'):
                    specs['flink-' + name] = {'group': lab.run_id + '-' + name + '-input', 'topic': inputs[name + '_input']}
                    specs['ksql-' + name] = {'contains': query_ids[name + '_table'], 'topic': inputs[name + '_input']}
            else:
                for name in ('compteur', 'indicateur'):
                    specs['flink-' + name] = {'group': lab.run_id + '-' + name + '-lookup', 'topic': outputs[name + '_table']['flink']}
                    specs['ksql-' + name] = {'contains': query_ids[name + '_output'], 'topic': outputs[name + '_table']['ksql']}
            lab.barrier(specs, phase)

        def facts(phase, base):
            records = []
            for index, case in enumerate(fixture[phase]):
                timestamp = origin + base + index * 1000
                records.append({'topic': inputs['facts'], 'key': None, 'value': case['value'],
                                'timestamp': timestamp, 'partition': 0})
                for key, value in case['expected'].items():
                    expected[key].append({'key': integer_key(case['value']['R_COMPTEURID']),
                                          'value': value, 'timestamp': timestamp})
            lab.produce(records, phase)
            barrier('facts', phase + '-facts-applied')

        def references(phase):
            records = []
            for key, changes in fixture[phase].items():
                for case in changes:
                    value = case['value']
                    partition = (value.get('ID') or 0) % 6 if value else 0
                    records.append({'topic': inputs[key], 'key': None, 'value': value,
                                    'timestamp': origin + case['time'] * 1000, 'partition': partition})
            lab.produce(records, phase)
            barrier('aggregate', phase + '-aggregated')
            barrier('lookup', phase + '-lookup-applied')

        facts('before', 100000)
        references('initial')
        facts('loaded', 200000)
        references('update')
        facts('changed', 300000)
        # Simulate a compacted-table deletion. It must clear lookup state, without retractions of past facts.
        deletes = []
        for key in ('compteur_table', 'indicateur_table'):
            for engine, topic in outputs[key].items():
                deletes.append({'topic': topic, 'key': integer_key(1), 'value': None,
                                'timestamp': origin + 400000, 'partition': None})
        lab.produce(deletes, 'delete-materialized-reference')
        barrier('lookup', 'delete-lookup-applied')
        facts('deleted', 500000)
        for key, history in fixture['table_expected'].items():
            expected[key] = [{'key': integer_key(row['id']), 'value': row['value'],
                              'timestamp': origin + row['time'] * 1000} for row in history]
            expected[key].append({'key': integer_key(1), 'value': None, 'timestamp': origin + 400000})
        count = lab.compare(outputs, expected)
        print(f'PASS lot 6: four queries, {count} changelog/fact outputs per engine; latest arrival, '
              f'non-retroactive joins, invalid references and deletes. {lab.artifact}')
    finally:
        lab.close()


if __name__ == '__main__':
    main()
