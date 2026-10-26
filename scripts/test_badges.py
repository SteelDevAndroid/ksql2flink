"""Four badge INSERT queries with correlated event and history fixtures."""
import base64
import json
from lab_pipeline import Pipeline
from smoke import ROOT


def main():
    lab = Pipeline('badge')
    manifest = json.loads((ROOT / 'docs/lots56-contracts.json').read_text())
    fixtures = json.loads((ROOT / 'fixtures/badges.json').read_text(encoding='utf-8'))
    topics, inputs, outputs = {}, {}, {}
    for direction in ('entree', 'sortie'):
        inputs[direction] = lab.topic(direction + '.input')
        topics[manifest['badge_sources'][direction]['topic']] = inputs[direction]
        outputs[direction] = {e: lab.topic(direction + '.' + e) for e in ('ksql', 'flink')}
        topics[manifest['badge_outputs'][direction]['topic']] = outputs[direction]['ksql']
    try:
        lab.reference('sql/ksqldb/05-badges.sql', topics, 4)
        flink_topics = {manifest['badge_sources'][d]['topic']: inputs[d] for d in inputs}
        flink_topics.update({manifest['badge_outputs'][d]['topic'] + '.flink': outputs[d]['flink'] for d in inputs})
        lab.launch('sql/flink/05-badges.sql', topics=flink_topics)
        records, expected = [], {d: [] for d in inputs}
        for source_index, (direction, topic) in enumerate(inputs.items()):
            for index, case in enumerate(fixtures[direction]):
                timestamp = 1792922400000 + source_index * 1000000 + index * 1000
                records.append({'topic': topic, 'key': base64.b64encode(b'ignored-input-key').decode(),
                                'value': case['value'], 'timestamp': timestamp, 'partition': index % 6})
                for row in case['expected']:
                    expected[direction].append({'key': None, 'value': row, 'timestamp': timestamp})
        lab.produce(records, 'badge-events')
        count = lab.compare(outputs, expected)
        print(f'PASS lot 5: {len(records)} inputs, {count} outputs per engine, correlated PAN/histo/D_VET. {lab.artifact}')
    finally:
        lab.close()


if __name__ == '__main__':
    main()
