-- Four correlated badge queries; a single UNNEST per matching event, never a Cartesian product.
CREATE TEMPORARY SYSTEM FUNCTION KSQL_PROJECT AS 'lab.ProjectKsqlJson';
CREATE TEMPORARY SYSTEM FUNCTION JSON_ITEMS AS 'lab.JsonElements';
CREATE TEMPORARY SYSTEM FUNCTION JSON_PARTS AS 'lab.JsonObjectParts';
CREATE TEMPORARY SYSTEM FUNCTION JSON_STRING AS 'lab.JsonString';
CREATE TEMPORARY SYSTEM FUNCTION BADGE_HISTORY AS 'lab.BadgeHistory';

CREATE TABLE badge_entree_input (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='va.peage.backoffice.conformite.entree.conforme',
  'properties.bootstrap.servers'='kafka:9092', 'properties.group.id'='badge-entree',
  'scan.startup.mode'='earliest-offset', 'format'='raw');
CREATE VIEW badge_entree AS SELECT KSQL_PROJECT(payload, 'entree') AS j, kafka_timestamp FROM badge_entree_input;
CREATE TABLE badge_entree_output (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='va.peage.histobadgeva.be.all.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw', 'sink.delivery-guarantee'='at-least-once');

CREATE TABLE badge_sortie_input (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='va.peage.backoffice.conformite.sortie.conforme',
  'properties.bootstrap.servers'='kafka:9092', 'properties.group.id'='badge-sortie',
  'scan.startup.mode'='earliest-offset', 'format'='raw');
CREATE VIEW badge_sortie AS SELECT KSQL_PROJECT(payload, 'sortie') AS j, kafka_timestamp FROM badge_sortie_input;
CREATE TABLE badge_sortie_output (
  payload STRING, kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH ('connector'='kafka', 'topic'='va.peage.histobadgeva.vs.all.flink',
  'properties.bootstrap.servers'='kafka:9092', 'format'='raw', 'sink.delivery-guarantee'='at-least-once');

EXECUTE STATEMENT SET
BEGIN
INSERT INTO badge_entree_output
SELECT JSON_PARTS(ARRAY['DHM','sens','localisation','PAN','histo','D_VET'], ARRAY[JSON_STRING(JSON_VALUE(j, '$.passageEntree.DHM')),JSON_STRING('E'),JSON_QUERY(j, '$.passageEntree.localisation'),JSON_STRING(JSON_VALUE(event_json, '$.paymentMeans.PAN')),BADGE_HISTORY(event_json,'CARDME'),JSON_QUERY(event_json,'$.D_VET')]), kafka_timestamp
FROM badge_entree CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.evenements'))) AS e(event_json)
WHERE (JSON_VALUE(j, '$.passageEntree.localisation.gare') IN ('25005601','25005306','25005304','25005603','25005302','25014360','25004201','25006068','25004334','25004276','25004205','25004204','25004179','25004182','25004183','25004181','25004180','25004170','25005532','25004541','25004987','25005055','25005057','25014380','25005318')) AND CHAR_LENGTH(JSON_VALUE(event_json, '$.paymentMeans.PAN'))>0 AND JSON_VALUE(event_json, '$.paymentMeans.PAN') NOT IN ('0000000000000000000','0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
  AND JSON_VALUE(event_json,'$.idEvenement')='DSRC.lecture.CARDME';

INSERT INTO badge_entree_output
SELECT JSON_PARTS(ARRAY['DHM','sens','localisation','PAN','histo','D_VET'], ARRAY[JSON_STRING(JSON_VALUE(j, '$.passageEntree.DHM')),JSON_STRING('E'),JSON_QUERY(j, '$.passageEntree.localisation'),JSON_STRING(JSON_VALUE(event_json, '$.F_IDU')),BADGE_HISTORY(event_json,'CIP'),JSON_QUERY(event_json,'$.D_VET')]), kafka_timestamp
FROM badge_entree CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.evenements'))) AS e(event_json)
WHERE (JSON_VALUE(j, '$.passageEntree.localisation.gare') IN ('25005601','25005306','25005304','25005603','25005302','25014360','25004201','25006068','25004334','25004276','25004205','25004204','25004179','25004182','25004183','25004181','25004180','25004170','25005532','25004541','25004987','25005055','25005057','25014380','25005318')) AND CHAR_LENGTH(JSON_VALUE(event_json, '$.F_IDU'))>0 AND JSON_VALUE(event_json, '$.F_IDU') NOT IN ('00000000000000000')
  AND JSON_VALUE(event_json,'$.idEvenement')='DSRC.lecture.CIP';

INSERT INTO badge_sortie_output
SELECT JSON_PARTS(ARRAY['DHM','sens','localisation','classe','tarif','PAN','histo','D_VET'], ARRAY[JSON_STRING(JSON_VALUE(j, '$.passageSortie.DHM')),JSON_STRING('S'),JSON_QUERY(j, '$.passageSortie.localisation'),JSON_STRING(JSON_VALUE(j,'$.passageSortie.tarification.classeTarifaire.classe')),JSON_VALUE(j,'$.passageSortie.tarification.prix.montantTTC'),JSON_STRING(JSON_VALUE(event_json, '$.paymentMeans.PAN')),BADGE_HISTORY(event_json,'CARDME'),JSON_QUERY(event_json,'$.D_VET')]), kafka_timestamp
FROM badge_sortie CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.evenements'))) AS e(event_json)
WHERE (JSON_VALUE(j, '$.passageSortie.localisation.gare') IN ('25005198','25004269','25004184','25004987','25004916','25021212','25021211','25021214','25005712','25005711','25005713') OR JSON_VALUE(j, '$.passageSortie.tarification.trajet.entree.gare') IN ('25005601','25005306','25005304','25005603','25005302','25014360','25004201','25006068','25004334','25004276','25004205','25004204','25004179','25004182','25004183','25004181','25004180','25004170','25005532','25004541','25004987','25005055','25005057','25014380','25012210','25012060','25007199','25009174','25009168','25009169','25009132','25009163','25009079','25022001','25005318')) AND CHAR_LENGTH(JSON_VALUE(event_json, '$.paymentMeans.PAN'))>0 AND JSON_VALUE(event_json, '$.paymentMeans.PAN') NOT IN ('0000000000000000000','0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
  AND JSON_VALUE(event_json,'$.idEvenement')='DSRC.lecture.CARDME';

INSERT INTO badge_sortie_output
SELECT JSON_PARTS(ARRAY['DHM','sens','localisation','classe','tarif','PAN','histo','D_VET'], ARRAY[JSON_STRING(JSON_VALUE(j, '$.passageSortie.DHM')),JSON_STRING('S'),JSON_QUERY(j, '$.passageSortie.localisation'),JSON_STRING(JSON_VALUE(j,'$.passageSortie.tarification.classeTarifaire.classe')),JSON_VALUE(j,'$.passageSortie.tarification.prix.montantTTC'),JSON_STRING(JSON_VALUE(event_json, '$.F_IDU')),BADGE_HISTORY(event_json,'CIP'),JSON_QUERY(event_json,'$.D_VET')]), kafka_timestamp
FROM badge_sortie CROSS JOIN UNNEST(JSON_ITEMS(JSON_QUERY(j,'$.evenements'))) AS e(event_json)
WHERE (JSON_VALUE(j, '$.passageSortie.localisation.gare') IN ('25005198','25004269','25004184','25004987','25004916','25021212','25021211','25021214','25005712','25005711','25005713') OR JSON_VALUE(j, '$.passageSortie.tarification.trajet.entree.gare') IN ('25005601','25005306','25005304','25005603','25005302','25014360','25004201','25006068','25004334','25004276','25004205','25004204','25004179','25004182','25004183','25004181','25004180','25004170','25005532','25004541','25004987','25005055','25005057','25014380','25012210','25012060','25007199','25009174','25009168','25009169','25009132','25009163','25009079','25022001','25005318')) AND CHAR_LENGTH(JSON_VALUE(event_json, '$.F_IDU'))>0 AND JSON_VALUE(event_json, '$.F_IDU') NOT IN ('00000000000000000')
  AND JSON_VALUE(event_json,'$.idEvenement')='DSRC.lecture.CIP';

END;
