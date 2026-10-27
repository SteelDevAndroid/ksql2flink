"""Generate lots 3/4 SQL and source contracts from the immutable recovery journal."""
import json
import re
from generate_conformite_sql import ROOT, SchemaParser


def main():
    log = (ROOT / 'kafka_streams_recovery.log').read_text()
    declarations = {}
    for m in re.finditer(r'^Statement\s*:\s*(.*?;)', log, re.M | re.S):
        q = m[1]
        name = re.search(r'CREATE (?:OR REPLACE )?(?:STREAM|TABLE) (\w+)', q)[1]
        declarations[name] = q
    sources = {'flows': 'VA_PEAGE_CLASSIFICATION_FLOWS'}
    targets = {'flows-extend': 'VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND',
               'compteur': 'VA_PEAGE_BACKOFFICE_SOCLEET_COMPTEUR'}
    for net in ('asf', 'esc'):
        for kind in ('coffre', 'bol'):
            sources[f'{net}-{kind}'] = f'{net.upper()}_PEAGE_SUIVICONSO_COMPTAGE_{kind.upper()}_BASE'
            targets[f'{net}-{kind}'] = f'{net.upper()}_PEAGE_SUIVICONSO_COMPTAGE_{kind.upper()}'
        targets[f'{net}-exploded'] = f'{net.upper()}_PEAGE_SUIVICONSO_COMPTAGE_BOL_EXPLODED'
        sources[f'{net}-synoptique'] = f'{net.upper()}_PEAGE_VOIE_SYNOPTIQUE_STREAM'
        targets[f'{net}-fav'] = f'{net.upper()}_PEAGE_TELEOP_SYNOPTIQUE_FAV_STREAM'
    # This source is the existing lot-6 output, not a replacement for its join.
    compteur_schema = {
        'R_VOIEID': 'INTEGER', 'R_GAREID': 'INTEGER', 'R_SOCIETEID': 'INTEGER',
        'R_PARTENAIREID': 'INTEGER', 'IDT_TRAN_PEA': 'STRING', 'VALEUR': 'DOUBLE',
        'MONTANT_TRANSACTION': 'DOUBLE', 'DATE_OPERATION': 'DOUBLE', 'DATE_COMPTABLE': 'DOUBLE',
        'R_FREQUENCEID': 'INTEGER', 'C_R_MODE_REGLEMENTID': 'INTEGER', 'R_COMPTEUR_TYPEID': 'INTEGER'}
    compteur_source = 'VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR'
    fields = ', '.join(f'{k} {v}' for k, v in compteur_schema.items())
    compteur_ddl = (f'CREATE STREAM {compteur_source} (R_COMPTEURID INTEGER KEY, {fields}) '
                    f"WITH (KAFKA_TOPIC='{compteur_source}', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');")
    reference, source_info, output_info = [], {}, {}
    for key, name in sources.items():
        ddl = declarations[name]
        reference.append(ddl)
        columns = ddl.split(' (', 1)[1].rsplit(') WITH', 1)[0]
        columns = columns.replace('KEY STRING KEY, ', '')
        parser = SchemaParser(columns)
        schema = parser.fields()
        if parser.index != len(parser.tokens):
            raise ValueError('Unconsumed source schema: ' + name)
        source_info[key] = {'name': name, 'topic': re.search(r"KAFKA_TOPIC='([^']+)'", ddl)[1],
                            'schema': schema, 'keyed': key.endswith('synoptique')}
    reference.append('-- Contract from DESCRIBE of the existing lot-6 output (join not migrated here).\n' + compteur_ddl)
    source_info['compteur-input'] = {'name': compteur_source, 'topic': compteur_source,
                                    'schema': compteur_schema, 'keyed': True}
    # Dependency order keeps both physical exploded topics in the reference pipeline.
    order = ['flows-extend', 'compteur', 'asf-coffre', 'esc-coffre',
             'asf-exploded', 'esc-exploded', 'asf-bol', 'esc-bol', 'asf-fav', 'esc-fav']
    for key in order:
        q = declarations[targets[key]]
        reference.append(q)
        retention = re.search(r'RETENTION_MS=(\d+)', q)
        output_info[key] = {'name': targets[key], 'topic': re.search(r"KAFKA_TOPIC='([^']+)'", q)[1],
                            'retention': int(retention[1]), 'keyed': key == 'compteur' or key.endswith('fav')}
    resource = ROOT / 'docker/flink/resources/schemas'
    for key, info in source_info.items():
        (resource / f'lot34-{key}.json').write_text(json.dumps(info['schema'], indent=2) + '\n')
    (ROOT / 'sql/ksqldb/04-lots34.sql').write_text('\n\n'.join(reference) + '\n')
    sql = ["""-- Lots 3/4. Inputs are existing Kafka contracts; outputs are isolated with .flink.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';
CREATE TEMPORARY SYSTEM FUNCTION JSON_ITEMS AS 'lab.JsonElements';
CREATE TEMPORARY SYSTEM FUNCTION JSON_PARTS AS 'lab.JsonObjectParts';
CREATE TEMPORARY SYSTEM FUNCTION INTEGER_KEY AS 'lab.IntegerKafkaKey';
CREATE TEMPORARY SYSTEM FUNCTION TRUNCATE_INTEGER AS 'lab.TruncateInteger';
CREATE TEMPORARY SYSTEM FUNCTION JSON_CONTAINS_STRUCT AS 'lab.JsonContains';
"""]
    def table(name, topic, keyed=False, source=False):
        key_column = 'kafka_key BYTES, ' if keyed else ''
        opts = ["'connector'='kafka'", f"'topic'='{topic}'", "'properties.bootstrap.servers'='kafka:9092'",
                "'value.format'='raw'"]
        if keyed:
            opts += ["'key.format'='raw'", "'key.fields'='kafka_key'", "'value.fields-include'='EXCEPT_KEY'"]
        if source:
            opts += ["'scan.startup.mode'='earliest-offset'", f"'properties.group.id'='lot34-{name}'"]
        else:
            opts += ["'sink.delivery-guarantee'='at-least-once'"]
        sql.append(f"CREATE TABLE {name} ({key_column}payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')\nWITH (" + ',\n  '.join(opts) + ');\n')
    for key, info in source_info.items():
        name = key.replace('-', '_')
        table(name + '_input', info['topic'], info['keyed'], True)
        keys = 'kafka_key, ' if info['keyed'] else ''
        sql.append(f"CREATE VIEW {name} AS SELECT {keys}kafka_timestamp, KSQL_PROJECT(payload, 'lot34-{key}') AS j FROM {name}_input;")
    for key, info in output_info.items():
        table(key.replace('-', '_') + '_output', info['topic'] + '.flink', info['keyed'])
    for net in ('asf', 'esc'):
        # The exploded topic is still published, but the bol query reuses the same view in-job
        # instead of reading it back from Kafka: same rows and timestamps, no extra hop.
        sql.append(f"CREATE VIEW {net}_exploded AS SELECT JSON_PARTS(ARRAY['message','comptageBols','Etat'], "
                   f"ARRAY[JSON_QUERY(j,'$.message'),JSON_QUERY(j,'$.comptageBols'),item]) AS j, kafka_timestamp "
                   f"FROM {net}_bol CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.etats'))) AS e(item);")
    def v(path, typ=None, doc='j'):
        suffix = f' RETURNING {typ}' if typ else ''
        return f"JSON_VALUE({doc}, '$.{path}'{suffix})"
    def obj(pairs):
        return 'JSON_OBJECT(' + ',\n  '.join(f"'{name}' VALUE {expr}" for name, expr in pairs) + ')'
    jobs = []
    def insert(key, expr, view, where=None):
        info = output_info[key]
        prefix = 'kafka_key, ' if info['keyed'] else ''
        jobs.append(f"INSERT INTO {key.replace('-', '_')}_output SELECT {prefix}{expr}, kafka_timestamp FROM {view}" +
                    (f' WHERE {where}' if where else '') + ';')
    # JSON_PARTS embeds the declared source structures instead of escaping them as strings.
    flow_fields = list(source_info['flows']['schema'])
    fields_sql = 'ARRAY[' + ','.join("'" + n + "'" for n in flow_fields + ['STATIONLANE']) + ']'
    values_sql = 'ARRAY[' + ','.join(f"JSON_QUERY(j, '$.{n}')" for n in flow_fields) + ",JSON_STRING(CONCAT(COALESCE(" + v('FLOW.STATION') + ",''),COALESCE(" + v('FLOW.LANE') + ",'')))]"
    # JSON_STRING is a small scalar wrapper registered below; it preserves NULL.
    sql.insert(1, "CREATE TEMPORARY SYSTEM FUNCTION JSON_STRING AS 'lab.JsonString';")
    insert('flows-extend', f'CASE WHEN j IS NULL THEN CAST(NULL AS STRING) ELSE JSON_PARTS({fields_sql}, {values_sql}) END', 'flows')
    compteur_pairs = [('R_COMPTEURID', 'INTEGER_KEY(kafka_key)')]
    compteur_pairs += [(k, v(k, 'INTEGER')) for k in ['R_VOIEID', 'R_GAREID', 'R_SOCIETEID', 'R_PARTENAIREID']]
    compteur_pairs += [('DATE_DEBUT', v('DATE_OPERATION', 'DOUBLE')), ('DATE_FIN', v('R_FREQUENCEID', 'INTEGER')),
                       ('DATE_COMPTABLE', v('DATE_COMPTABLE', 'DOUBLE')), ('VALEUR', v('VALEUR', 'DOUBLE'))]
    insert('compteur', 'CASE WHEN j IS NULL THEN CAST(NULL AS STRING) ELSE ' + obj(compteur_pairs) + ' END', 'compteur_input')
    for net, gare in [('asf', '25004'), ('esc', '25006')]:
        root = 'comptageCoffre'
        pos, kind = v(root + '.position', 'INTEGER'), v(root + '.contenu.type')
        position = f"CASE {pos} WHEN 0 THEN 15 WHEN 1 THEN CASE {kind} WHEN 'piece' THEN 7 WHEN 'billet' THEN 16 ELSE 1 END WHEN 2 THEN 8 WHEN 5 THEN 14 ELSE {pos} END"
        pairs = [('DateMessage', v('message.DHM')), ('IdGare', v(root + '.localisation.gare')),
                 ('Voie', v(root + '.localisation.equipement.numero')), ('DateComptage', v(root + '.dhm')),
                 ('Position', position), ('Type', v(root + '.type')), ('TypeEspece', kind)]
        for alias, path, typ in [('ValeurCapacite', 'capacite.valeur', 'INTEGER'), ('UniteCapacite', 'capacite.unite', None),
                                 ('ValeurCourant', 'courant.valeur', 'INTEGER'), ('UniteCourant', 'courant.unite', None)]:
            pairs.append((alias, v(root + '.contenu.' + path, typ)))
        insert(net + '-coffre', obj(pairs), net + '_coffre',
               f"{v(root+'.type')} IN ('sac','coffre') AND {v(root+'.localisation.gare')} LIKE '{gare}%'")
        insert(net + '-exploded', 'j', net + '_exploded')
        amount = v('Etat.contenu.valeur', 'DOUBLE')
        position = f'CASE {amount} ' + ' '.join(f'WHEN {a} THEN {n}' for a, n in [(0.1,9),(0.2,10),(0.5,11),(1,12),(2,13)]) + f' ELSE TRUNCATE_INTEGER({amount}) END'
        pairs = [('DateMessage', v('message.DHM')), ('IdGare', v('comptageBols.localisation.gare')),
                 ('Voie', v('comptageBols.localisation.equipement.numero')), ('DateComptage', v('comptageBols.dhm')),
                 ('Position', position), ('Numero', f"SPLIT_INDEX({v('Etat.sujet')}, '.', 1)"),
                 ('Type', v('Etat.contenu.type')), ('Devise', v('Etat.contenu.devise')),
                 ('Valeur', v('Etat.contenu.courant', 'INTEGER')), ('Capacite', v('Etat.contenu.maximum', 'INTEGER'))]
        condition = f"{v('Etat.contenu.type')}='piece' AND {v('comptageBols.localisation.gare')} LIKE '{gare}%'"
        if net == 'esc':
            condition += f" AND {v('comptageBols.localisation.equipement.type')} <> 'MRB'"
        insert(net + '-bol', obj(pairs), net + '_exploded', condition)
        def has(subject, state):
            return f"JSON_CONTAINS_STRUCT(JSON_QUERY(j,'$.ETATEQUIPEMENTS'), JSON_OBJECT('SUJET' VALUE '{subject}','ETAT' VALUE '{state}'))"
        states = [('COMLSKO','comLS',[('comKO',1),('comOKNTR',0),('comOKTR',0)]),
                  ('CONTRESENS','contreSens',[('actif',1),('nonActif',0)]),
                  ('FAVFLECHEVERTE','feu.affectation.flecheVerte',[('allume',1),('eteint',0)]),
                  ('FAVTIS','feu.affectation.TIS',[('allume',1),('eteint',0)]),
                  ('FAVCB','feu.affectation.carte',[('allume',1),('eteint',0)]),
                  ('PAIEMENTESPECE','paiement.espece',[('accepte',1),('nonAccepte',0)])]
        equipment = v('SYNOPTIQUE.LOCALISATION.EQUIPEMENT.TYPE')
        entrance = "'BE','BESA','BET'" + (",'BE ESCOTA'" if net == 'esc' else '')
        group = '1'
        if net == 'esc':
            stations = re.search(r'ARRAY\[(.*?)\]', declarations[targets['esc-fav']])[1]
            number = f"CAST({v('SYNOPTIQUE.LOCALISATION.EQUIPEMENT.NUMERO')} AS INT)"
            group = f"CASE WHEN {v('SYNOPTIQUE.LOCALISATION.GARE')} IN ({stations}) THEN CASE WHEN {number}<20 THEN 1 ELSE 2 END ELSE 1 END"
        pairs = [('DHM', v('SYNOPTIQUE.DHM')), ('GARE', v('SYNOPTIQUE.LOCALISATION.GARE')),
                 ('VOIE', v('SYNOPTIQUE.LOCALISATION.EQUIPEMENT.NUMERO')), ('GAREGROUP', group),
                 ('ENTREESORTIE', f"CASE WHEN {equipment} IN ({entrance}) THEN 'E' ELSE 'S' END")]
        for alias, subject, options in states:
            pairs.append((alias, 'CASE ' + ' '.join(f'WHEN {has(subject,s)} THEN {value}' for s,value in options) + ' ELSE CAST(NULL AS INT) END'))
        condition = ' OR '.join(has(subject,s) for _,subject,options in states for s,_ in options)
        condition = '(' + condition + ')'
        if net == 'asf':
            condition = f"NOT COALESCE({equipment} IN ('BECM','VSP'), FALSE) AND " + condition
        insert(net + '-fav', obj(pairs), net + '_synoptique', condition)
    sql += ['EXECUTE STATEMENT SET\nBEGIN', '\n\n'.join(jobs), 'END;\n']
    (ROOT / 'sql/flink/04-lots34.sql').write_text('\n'.join(sql))
    manifest = {'sources': source_info, 'outputs': output_info}
    (ROOT / 'docs/lots34-contracts.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
