"""Differential test of ten lot-3/4 journal queries at their Kafka boundaries."""
import json
import re
import time
import uuid
from flink_kafka import resolve
from smoke import ROOT, docker, request, canonical
from test_lexhistoric import probe


def main():
    manifest = json.loads((ROOT / 'docs/lots34-contracts.json').read_text())
    fixtures = json.loads((ROOT / 'fixtures/lots34.json').read_text(encoding='utf-8'))
    run_id = 'lot34' + uuid.uuid4().hex[:8]
    artifact = ROOT / 'artifacts' / run_id
    artifact.mkdir(parents=True)
    ksql = (ROOT / 'sql/ksqldb/04-lots34.sql').read_text()
    flink = resolve((ROOT / 'sql/flink/04-lots34.sql').read_text())
    inputs, outputs = {}, {}
    for key, info in manifest['sources'].items():
        inputs[key] = f'{run_id}.input.{key}'
    for key in manifest['outputs']:
        outputs[key] = {e: f'{run_id}.{e}.{key}' for e in ('ksql','flink')}
    # Replace whole SQL topic literals only: bol exploded topics are also input boundaries.
    for key, info in manifest['outputs'].items():
        ksql = ksql.replace("'" + info['topic'] + "'", "'" + outputs[key]['ksql'] + "'")
        flink = flink.replace("'" + info['topic'] + ".flink'", "'" + outputs[key]['flink'] + "'")
    for key, info in manifest['sources'].items():
        ksql = ksql.replace("'" + info['topic'] + "'", "'" + inputs[key] + "'")
        flink = flink.replace("'" + info['topic'] + "'", "'" + inputs[key] + "'")
    names = [i['name'] for i in manifest['sources'].values()] + [i['name'] for i in manifest['outputs'].values()]
    for name in sorted(set(names), key=len, reverse=True):
        ksql = re.sub(r'\b' + name + r'\b', run_id.upper() + '_' + name, ksql)
    ksql = re.sub(r'REPLICAS=\d+', 'REPLICAS=1', ksql).replace("CLEANUP_POLICY='delete', ", '')
    for topic in inputs.values():
        docker('exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:9092','--create',
               '--topic',topic,'--partitions','6','--replication-factor','1')
    for key, engines in outputs.items():
        for topic in engines.values():
            docker('exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:9092','--create',
                   '--topic',topic,'--partitions','6','--replication-factor','1',
                   '--config','cleanup.policy=delete','--config',f'retention.ms={manifest["outputs"][key]["retention"]}')
    query_ids, job_id = [], None
    try:
        (artifact/'ksql.sql').write_text(ksql)
        request('http://localhost:8088/ksql', {'ksql':ksql,'streamsProperties':{'auto.offset.reset':'earliest'}})
        queries=request('http://localhost:8088/ksql',{'ksql':'SHOW QUERIES;','streamsProperties':{}})
        query_ids=[q['id'] for item in queries for q in item.get('queries',[]) if run_id.upper() in q.get('queryString','')]
        if len(query_ids)!=10: raise AssertionError(f'Expected ten queries, got {query_ids}')
        path=artifact/'flink.sql';path.write_text(flink)
        docker('cp',str(path),'jobmanager:/tmp/lots34.sql')
        output=docker('exec','-T','jobmanager','/opt/flink/bin/sql-client.sh','embedded','-f','/tmp/lots34.sql')
        (artifact/'flink-submit.log').write_text(output,encoding='utf-8')
        match=re.search(r'Job ID:\s*([0-9a-f]{32})',output)
        if not match or '[ERROR]' in output: raise RuntimeError(output)
        job_id=match[1];deadline=time.monotonic()+90
        while True:
            state=request('http://localhost:8081/jobs/'+job_id)['state']
            if state=='RUNNING':break
            if state in ('FAILED','CANCELED','FINISHED') or time.monotonic()>deadline:raise RuntimeError('Flink state: '+state)
            time.sleep(1)
        expected={key:[] for key in outputs};input_count=0
        for source_index,(key,topic) in enumerate(inputs.items()):
            records=[]
            for index,case in enumerate(fixtures[key]):
                timestamp=1792922400000+source_index*1000000+index*1000
                records.append({'key':case.get('key'),'value':case['value'],'timestamp':timestamp,'partition':index%6})
                for out_key, rows in case['expected'].items():
                    for row in rows:
                        expected[out_key].append({'key':row.get('key'),'value':row['value'],'timestamp':timestamp})
                input_count+=1
            path=artifact/(key+'.jsonl')
            path.write_text('\n'.join(json.dumps(r,ensure_ascii=False) for r in records),encoding='utf-8')
            docker('cp',str(path),'jobmanager:/tmp/lots34-input.jsonl')
            probe('produce',topic,'/tmp/lots34-input.jsonl')
        actual={key:{} for key in outputs}
        for engine in ('ksql','flink'):
            topic_map={topics[engine]:key for key,topics in outputs.items()}
            grouped={key:[] for key in outputs}
            for row in probe('consume-many',','.join(topic_map)):
                key=topic_map[row.pop('topic')];grouped[key].append(row)
            for key,rows in grouped.items():actual[key][engine]=rows
        report={'run_id':run_id,'input_count':input_count,'expected':expected,'actual':actual}
        (artifact/'comparison.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
        errors=[]
        for key,engines in actual.items():
            for engine,rows in engines.items():
                missing=canonical(expected[key])-canonical(rows);extra=canonical(rows)-canonical(expected[key])
                if missing or extra:errors.append(f'{key}/{engine}: missing={sum(missing.values())} extra={sum(extra.values())}')
        if errors:raise AssertionError('; '.join(errors)+f'; report={artifact}')
        print(f'PASS: {input_count} inputs, ten queries, {sum(map(len,expected.values()))} outputs per engine. {artifact}')
    finally:
        if job_id:request('http://localhost:8081/jobs/'+job_id+'?mode=cancel',method='PATCH')
        for query_id in query_ids:request('http://localhost:8088/ksql',{'ksql':f'TERMINATE {query_id};','streamsProperties':{}})


if __name__=='__main__':main()
