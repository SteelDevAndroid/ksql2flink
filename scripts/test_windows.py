"""Differential tests of all five queries, including each materialized Kafka boundary.

Stages are isolated so the alarm oracle has identical, ordered input histories on both
engines. Concurrent upstream joins have no deterministic cross-topic arrival order.
"""
import base64
import json
import struct
import sys
from lab_pipeline import Pipeline
from smoke import ROOT, canonical
from test_lexhistoric import probe

ORIGIN = 1792922400000  # exact UTC hour
HOUR = 3600000


def key_sequences(rows):
    """Keep changelog order within each physical key, ignoring cross-key scheduling."""
    grouped={}
    for row in rows:
        grouped.setdefault((row['partition'],row['key']),[]).append(row)
    return grouped


def record(value, time=0, key=None):
    return {'value': value, 'timestamp': ORIGIN + time, 'key': key, 'partition': 0}


def quality(station='FR25004', lane='01', origin='Manual', caller='AUTO', classes=None, date='2026-10-24T12:00:00.1234567Z'):
    return {'FLOW': {'STATION': station, 'LANE': lane, 'CREATIONDATE': date, 'CALLERMODE': caller,
                     'CLASS': classes if classes is not None else [{'CLASSTYPE': 'TarifiedClass', 'CLASSORIGIN': origin}]},
            'STATIONLANE': (station or '') + (lane or '')}


def quality_update(lane='01', countkey=1, date='2026-10-24T12:00:00.1234567Z', total=20):
    return {'STATION': 'FR25004', 'LANE': lane, 'STATIONLANE': 'FR25004' + lane,
            'CREATIONDATE': date, 'NBCLASSIFWRONGORIGIN': total, 'COUNTKEY': countkey}


def scenarios(stage):
    if stage == 'hourly':
        key = base64.b64encode(struct.pack('>i', -1)).decode()
        v = {'R_VOIEID': -2, 'R_GAREID': -3, 'DATE_OPERATION': -4.0,
             'R_FREQUENCEID': -5, 'DATE_COMPTABLE': -6.0, 'VALEUR': 2.5}
        return [ [('indicator_input', record(dict(v, VALEUR=None), 0, key)),
                  ('indicator_input', record(v, 1000, key)), ('indicator_input', record(v, 1000, key)),
                  ('indicator_input', record(dict(v, VALEUR=-1.0), 500, key)),
                  ('indicator_input', record(v, HOUR-1, key)), ('indicator_input', record(v, HOUR, key)),
                  ('indicator_input', record(dict(v, R_GAREID=None), 2000, key)),
                  ('indicator_input', record(None, 3000, key)), ('indicator_input', record(v, 3000))],
                 [('indicator_input', record(v, 26*HOUR, key))],
                 [('indicator_input', record(v, 0, key)), ('indicator_input', record(v, 2*HOUR, key))] ]
    if stage == 'quality':
        return [ [('flows', record(quality(), i*1000)) for i in range(22)],
                 [('flows', record(quality(date=None), 20000)),
                  ('flows', record(quality(caller='TD'), 22000)), ('flows', record(quality(caller=None),22000)),
                  ('flows', record(quality(classes=[]),22000)), ('flows', record(quality(classes=[{'CLASSTYPE':'TarifiedClass'}, {'CLASSTYPE':'PostDTO'}]),22000)),
                  ('flows', record(quality(classes=[{'CLASSTYPE':'TarifiedClass'}, None]),22000)),
                  ('flows', record(quality(station=None),22000)),
                  ('flows', record(None,22000))],
                 [('flows',record(quality(lane='02',origin='Auto'),i*1000)) for i in range(20)],
                 [('flows',record(quality(lane='02',origin='FC'),20000+i*1000)) for i in range(9)],
                 [('flows',record(quality(lane='02',origin='Auto'),30000+i*1000)) for i in range(2)],
                 [('flows',record(quality(),3*HOUR+i*1000)) for i in range(20)],
                 [('flows',record(quality(origin='FC',date='2026-10-25T01:30:00.0000000Z'),HOUR)),
                  ('flows',record(quality(origin='FC'),2*HOUR))],
                 [('flows',record(quality(),30*HOUR))],
                 [('flows',record(quality(),0))] ]
    if stage == 'count':
        return [ [('quality',record(quality_update(lane=f'{i:02}'),0)) for i in range(31)],
                 [('quality',record(quality_update(lane='01'),1000)),('quality',record(None,2000)),
                  ('quality',record(dict(quality_update(),STATIONLANE=None),2000)),
                  ('quality',record(quality_update(countkey=None),2000))],
                 [('quality',record(quality_update(lane='32'),3*HOUR))],
                 [('quality',record(quality_update(lane='33'),HOUR)), ('quality',record(quality_update(lane='34'),2*HOUR))],
                 [('quality',record(quality_update(),30*HOUR))], [('quality',record(quality_update(),0))] ]
    # Ordered, bounded many-to-many joins, 60-second boundaries, threshold switching,
    # old timestamps, independent keys, null LATEST values, and Paris DST conversion.
    def q(t=0, **kw): return ('quality',record(quality_update(**kw),t))
    def c(n,t=0,key=1): return ('count',record({'NBTOTALVOIE':n,'COUNTKEYVALUE':key},t))
    return [[q()], [c(29)], [c(30,60000)], [q(60000,lane='02',date='2026-10-25T01:30:00.0000000Z',total=21)],
            [c(31,60001)], [q(0,total=22)], [q(60000,date=None,total=None)], [c(None,60000)],
            [c(1,120001)], [q(180002,lane='03')], [c(1,180002,key=2)],
            [('quality',record(None,180002)),('count',record(None,180002))]]


def run(stage):
    lab = Pipeline('win' + stage)
    manifest = json.loads((ROOT / 'docs/lots78-contracts.json').read_text())
    needed = {'hourly': ['indicator_input','hourly'], 'quality':['flows','quality'],
              'count':['quality','count'], 'alarms':['quality','count','unit_alarm','global_alarm']}[stage]
    input_names = needed[:1] if stage != 'alarms' else needed[:2]
    output_names = needed[1:] if stage != 'alarms' else needed[2:]
    inputs = {k:lab.topic(k) for k in input_names}
    outputs = {}
    for k in output_names:
        # Both alarm queries intentionally write to the same physical topic.
        if k == 'global_alarm': outputs[k] = outputs['unit_alarm']
        else: outputs[k] = {e:lab.topic(k+'.'+e) for e in ('ksql','flink')}
    topics = {manifest[k]['topic']:topic for k,topic in inputs.items()}
    topics.update({manifest[k]['topic']:ts['ksql'] for k,ts in outputs.items()})
    config = dict(inputs,group_prefix=lab.run_id,stages=[stage])
    config.update({k:ts['flink'] for k,ts in outputs.items()})
    try:
        lab.reference(f'sql/ksqldb/07-08-{stage}.sql',topics,2 if stage=='alarms' else 1)
        lab.launch(None,config=config,main_class='lab.WindowJob')
        specs={}
        for k,topic in inputs.items():
            suffix='indicator-input' if k=='indicator_input' else k
            specs['flink-'+k]={'group':lab.run_id+'-'+suffix,'topic':topic}
            for q in lab.queries: specs['ksql-'+q['id']+'-'+k]={'contains':q['id'],'topic':topic}
        active_topics=set()
        for i,phase in enumerate(scenarios(stage)):
            lab.produce([dict(row,topic=inputs[k]) for k,row in phase],f'phase-{i}')
            active_topics.update(inputs[k] for k,row in phase)
            # A Flink source using manual partition assignment has no consumer-group
            # metadata until its first offset commit. Empty inputs need no barrier.
            lab.barrier({name:spec for name,spec in specs.items() if spec['topic'] in active_topics},f'phase-{i}')
        topics_out=next(iter(outputs.values()))
        rows={engine:probe('consume-partitioned',topic) for engine,topic in topics_out.items()}
        (lab.artifact/'comparison.json').write_text(json.dumps(rows,indent=2,ensure_ascii=False),encoding='utf-8')
        missing=canonical(rows['ksql'])-canonical(rows['flink'])
        extra=canonical(rows['flink'])-canonical(rows['ksql'])
        if missing or extra:
            raise AssertionError(f'{stage}: missing={sum(missing.values())}, extra={sum(extra.values())}; {lab.artifact}')
        assert key_sequences(rows['ksql'])==key_sequences(rows['flink']),f'{stage}: per-key changelog order differs; {lab.artifact}'
        values=[r['value'] for r in rows['ksql'] if r['value'] is not None]
        assert values, 'No output: the oracle must exercise the query'
        if stage=='hourly':
            assert values[0]['VALEUR']==0.0
            assert any(v['VALEUR']==4.0 for v in values)
            assert len(rows['ksql'])==7, len(rows['ksql'])
        elif stage=='quality':
            assert any(v['NBPASSAGE']>=40 for v in values), 'Missing session merge'
            assert any(r['value'] is None for r in rows['ksql']), 'Missing session tombstones'
        elif stage=='count':
            assert any(v['NBTOTALVOIE']==30 for v in values)
            assert max(v['NBTOTALVOIE'] for v in values)>=31
            assert any(r['value'] is None for r in rows['ksql'])
        else:
            assert any(v['LIBELLEENSEMBLEEQUIPEMENT']=='Société' for v in values)
            assert any(v['LIBELLEENSEMBLEEQUIPEMENT']=='Voie' for v in values)
            assert any(v['HORODATEALARME']=='2026-10-25T02:30:00Z' for v in values)
        print(f'PASS {stage}: {len(rows["ksql"])} outputs/engine, raw keys, timestamps, multiplicity and tombstones. {lab.artifact}',flush=True)
    finally:
        lab.close()


def pipeline():
    """Exercise the complete deployment with a single, unambiguous join pair."""
    lab=Pipeline('winpipeline')
    manifest=json.loads((ROOT/'docs/lots78-contracts.json').read_text())
    inputs={k:lab.topic(k) for k in ('indicator_input','flows')}
    outputs={k:{e:lab.topic(k+'.'+e) for e in ('ksql','flink')} for k in ('hourly','quality','count','unit_alarm')}
    outputs['global_alarm']=outputs['unit_alarm']
    topics={v['topic']:inputs[k] if v['source'] else outputs[k]['ksql'] for k,v in manifest.items()}
    config=dict(inputs,group_prefix=lab.run_id)
    config.update({k:ts['flink'] for k,ts in outputs.items()})
    try:
        lab.reference('sql/ksqldb/07-08-windows.sql',topics,5)
        lab.launch(None,config=config,main_class='lab.WindowJob')
        key=base64.b64encode(struct.pack('>i',1)).decode()
        records=[dict(record(quality()),topic=inputs['flows']) for _ in range(20)]
        records.append(dict(record({'R_VOIEID':2,'R_GAREID':3,'DATE_OPERATION':4.0,'R_FREQUENCEID':5,'DATE_COMPTABLE':6.0,'VALEUR':2.5},key=key),topic=inputs['indicator_input']))
        lab.produce(records,'input')
        for level,source_names in enumerate([['indicator_input','flows'],['quality'],['count']]):
            specs={}
            for k in source_names:
                suffix='indicator-input' if k=='indicator_input' else k
                topic=inputs[k] if k in inputs else outputs[k]['flink']
                specs['flink-'+k]={'group':lab.run_id+'-'+suffix,'topic':topic}
            for q in lab.queries:
                name=next(k for k,v in manifest.items() if not v['source'] and ('_'+v['name']+' ') in q['queryString'])
                source={'hourly':'indicator_input','quality':'flows','count':'quality','unit_alarm':'count','global_alarm':'count'}[name]
                if source in source_names:
                    specs['ksql-'+name]={'contains':q['id'],'topic':inputs[source] if source in inputs else outputs[source]['ksql']}
            lab.barrier(specs,f'level-{level}')
        report={}
        for k in ('hourly','quality','count','unit_alarm'):
            rows={e:probe('consume-partitioned',topic) for e,topic in outputs[k].items()}
            report[k]=rows
            (lab.artifact/'comparison.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
            assert canonical(rows['ksql'])==canonical(rows['flink']),f'{k} pipeline mismatch: {lab.artifact}'
            assert key_sequences(rows['ksql'])==key_sequences(rows['flink']),f'{k}: per-key order differs; {lab.artifact}'
            assert len(rows['ksql'])==1,f'{k}: expected exactly one output, found {len(rows["ksql"])}'
        (lab.artifact/'comparison.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
        print(f'PASS pipeline: five queries connected through their physical Kafka topics, four outputs/engine. {lab.artifact}',flush=True)
    finally:
        lab.close()


if __name__=='__main__':
    for stage in sys.argv[1:] or ['hourly','quality','count','alarms','pipeline']:
        pipeline() if stage=='pipeline' else run(stage)
