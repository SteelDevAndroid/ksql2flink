# Les requêtes du laboratoire dans Kafka Explorer

[Kafka Explorer](https://github.com/devdownin/Kafkaexplorer) exécute du Flink SQL sur des topics
Kafka à travers un moteur Flink embarqué. Ce document reprend six traitements du laboratoire sous
une forme que ce moteur accepte, avec les données sur lesquelles les exécuter et les résultats
attendus. Les mêmes requêtes figurent dans la section « ksqlDB → Flink migration » de son
`docs/QUERY-EXAMPLES.md`, où un test les vérifie contre les topics du script de seed.

Ce ne sont pas les jobs de migration : ceux-ci restent dans `sql/flink` et sont comparés à ksqlDB
par les scripts de `scripts/`. Ce sont leurs lectures, pour explorer les données et vérifier une
règle de gestion sans soumettre de job.

## Données

`scripts/seed_kafka_explorer.sh` crée les cinq topics et y produit 148 messages. Avec le
laboratoire démarré :

```powershell
docker compose cp scripts/seed_kafka_explorer.sh kafka:/tmp/seed_kafka_explorer.sh
docker compose exec kafka bash /tmp/seed_kafka_explorer.sh kafka:9092
```

Kafka Explorer se connecte ensuite à `localhost:29092`. Les topics sont créés avec
`--if-not-exists` ; les tests de comparaison isolent leurs propres topics et ne les lisent pas.

| Topic | Messages | Clé | Contenu |
|---|---|---|---|
| `asf.peage.voie.passagesortieva` | 16 | `IDPASSAGE` | Passages de sortie, cinq types de paiement en casses mêlées, 3 en espèces, 1 avec deux paiements |
| `esc.peage.voie.passagesortieva` | 8 | `IDPASSAGE` | Même forme pour une seconde société, 2 en espèces |
| `va.peage.backoffice.socleET.ref_compteur` | 6 | `ID` | Cinq compteurs ; l'ID 3 publié deux fois, l'ID 5 sans fréquence |
| `va.peage.backoffice.socleET.etancheite` | 48 | `IDT_TRAN_PEA` | Une transaction toutes les cinq minutes sur les quatre heures précédant le seed |
| `VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND` | 70 | `STATIONLANE` | Flux de classification sur trois voies, plus 4 flux `TD` et 4 `PostDTO` à exclure |

Un passage de sortie tel que le porte `asf.peage.voie.passagesortieva` :
```json
{
  "IDPASSAGE": "ASF-00001",
  "MESSAGE": {"NUMERO": 1, "TYPE": "PASSAGESORTIE", "DHM": "2026-10-25T08:01:00Z", "TYPEEMISSION": "TR",
              "PRODUCTEUR": {"TYPE": "VOIE", "ID": "25006123"}},
  "PASSAGESORTIE": {
    "SYSTEME": "ASF", "DHM": "2026-10-25T08:01:00Z", "STATUT": "VALIDE",
    "LOCALISATION": {"GARE": "25006123", "EQUIPEMENT": {"NUMERO": "2", "TYPE": "VS"}},
    "TARIFICATION": {
      "CLASSETARIFAIRE": {"CLASSE": "2", "ACQUISITION": "AUTO"},
      "PRIX": {"MONTANTHT": 3.52, "MONTANTTVA": 0.71, "MONTANTTTC": 4.23, "TAUXTVA": 20.00}
    },
    "PAIEMENT": {"RETENUS": [{"TYPE": "telepeagecardme", "MONTANT": 4.23, "DEVISE": "EUR", "PAN": "PAN-ASF-1"}]}
  }
}
```

## Ce qui change par rapport au SQL du laboratoire

Le laboratoire soumet des `INSERT INTO` à l'aide de `SET`, `CREATE VIEW` et d'UDF Java ; Kafka
Explorer ne fait que lire, donc chaque job devient le `SELECT` qui l'alimente. Trois réécritures
reviennent :

- **Les champs `STRUCT` sont des chaînes JSON.** L'inférence de schéma type un objet imbriqué en
  `STRING`, donc `PASSAGESORTIE->PAIEMENT->RETENUS[1]->TYPE` devient
  `JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE')` — les tableaux ksqlDB commencent à 1,
  ceux d'un chemin JSON à 0.
- **`LATEST_BY_OFFSET` devient `LAST_VALUE`**, groupé par la clé : c'est ainsi qu'un stream
  devient la `TABLE` ksqlDB qu'une jointure lit.
- **`REDUCE` sur un tableau n'a pas d'équivalent sans UDF** (le laboratoire fournit
  `JsonContains`) ; `JSON_QUERY` sur le champ du tableau, puis `LIKE`, répond à la même question :
  un élément porte-t-il cette valeur.

Les clés insensibles à la casse (`KSQL_JSON`, `lab.NormalizeJsonKeys`) n'ont pas d'équivalent
non plus : les données du seed écrivent leurs clés en majuscules, comme le journal les déclare.

## Lot 1 — Lexhistoric

Origine : [`sql/ksqldb/02-lexhistoric.sql`](../sql/ksqldb/02-lexhistoric.sql),
[`sql/flink/02-lexhistoric.sql`](../sql/flink/02-lexhistoric.sql). Les passages d'une société
projetés vers le mode de règlement.
```sql
SELECT IDPASSAGE AS IDTRAN,
       JSON_VALUE(PASSAGESORTIE, '$.DHM') AS DATETRANSACTION,
       JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].PAN') AS CONTENUISO,
       CASE LOWER(JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE'))
         WHEN 'telepeagelibert' THEN 256
         WHEN 'telepeagecardme' THEN 512
         WHEN 'carteaccreditive' THEN 1024
         WHEN 'cartemagnetique' THEN 2048
       END AS MODEREGLEMENT,
       SUBSTRING(JSON_VALUE(PASSAGESORTIE, '$.LOCALISATION.GARE'), 1, 5) AS SCA
FROM "asf.peage.voie.passagesortieva"
WHERE LOWER(JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE'))
      IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique');
```
Les passages écartés par le filtre, d'après leur première entrée `RETENUS` :

| `IDPASSAGE` | `PAIEMENT.RETENUS` | Raison |
|---|---|---|
| `ASF-00004`, `ASF-00009`, `ASF-00014` | `[{"TYPE":"especes", …}]` | Les espèces ne sont pas un mode de règlement retenu |
| `ASF-00016` | `[{"TYPE":"especes", …}, {"TYPE":"telepeagelibert", …}]` | Seul le premier paiement est lu |

Douze lignes, dont les premières :

| IDTRAN | DATETRANSACTION | CONTENUISO | MODEREGLEMENT | SCA |
|---|---|---|---|---|
| ASF-00001 | 2026-10-25T08:01:00Z | PAN-ASF-1 | 512 | 25006 |
| ASF-00002 | 2026-10-25T08:02:00Z | PAN-ASF-2 | 1024 | 25021 |
| ASF-00003 | 2026-10-25T08:03:00Z | PAN-ASF-3 | 2048 | 25005 |
| ASF-00005 | 2026-10-25T08:05:00Z | PAN-ASF-5 | 256 | 25006 |
| … | | | | |

La consolidation : ksqlDB écrit un `CREATE STREAM … AS SELECT` par société dans un topic commun,
Flink les lit comme un seul `UNION ALL`. Dix-huit passages de deux sociétés, comptés par mode.
```sql
WITH passages AS (
  SELECT IDPASSAGE, PASSAGESORTIE FROM "asf.peage.voie.passagesortieva"
  UNION ALL
  SELECT IDPASSAGE, PASSAGESORTIE FROM "esc.peage.voie.passagesortieva"
)
SELECT JSON_VALUE(PASSAGESORTIE, '$.SYSTEME') AS SOCIETE,
       LOWER(JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE')) AS MODE_REGLEMENT,
       COUNT(*) AS NB_PASSAGES
FROM passages
WHERE LOWER(JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE'))
      IN ('telepeagelibert', 'telepeagecardme', 'carteaccreditive', 'cartemagnetique')
GROUP BY JSON_VALUE(PASSAGESORTIE, '$.SYSTEME'),
         LOWER(JSON_VALUE(PASSAGESORTIE, '$.PAIEMENT.RETENUS[0].TYPE'));
```
| SOCIETE | MODE_REGLEMENT | NB_PASSAGES |
|---|---|---|
| ASF | telepeagelibert | 3 |
| ASF | telepeagecardme | 3 |
| ASF | carteaccreditive | 3 |
| ASF | cartemagnetique | 3 |
| ESC | telepeagelibert | 2 |
| ESC | telepeagecardme | 1 |
| ESC | carteaccreditive | 1 |
| ESC | cartemagnetique | 2 |

## Lot 6 — socle ET

Origine : [`sql/ksqldb/06-socleet.sql`](../sql/ksqldb/06-socleet.sql) ; le laboratoire migre ce
lot en job DataStream (`lab.SocleEtJob`, [`sql/flink/06-socleet.json`](../sql/flink/06-socleet.json)).

Le référentiel compteurs tel que la table ksqlDB `TABLE_VA_PEAGE_BACKOFFICE_SOCLEET_REF_COMPTEURS`
le tient. Les six messages, dans l'ordre des offsets :
```json
{"ID":1,"R_COMPTEUR_TYPEID":1,"R_FREQUENCEID":1,"R_OPERATIONID":10,"R_MODE_REGLEMENTID":1,"R_TRANSACTION_ETATID":1}
{"ID":2,"R_COMPTEUR_TYPEID":2,"R_FREQUENCEID":1,"R_OPERATIONID":10,"R_MODE_REGLEMENTID":256,"R_TRANSACTION_ETATID":1}
{"ID":3,"R_COMPTEUR_TYPEID":1,"R_FREQUENCEID":2,"R_OPERATIONID":20,"R_MODE_REGLEMENTID":1,"R_TRANSACTION_ETATID":2}
{"ID":4,"R_COMPTEUR_TYPEID":2,"R_FREQUENCEID":2,"R_OPERATIONID":20,"R_MODE_REGLEMENTID":512,"R_TRANSACTION_ETATID":1}
{"ID":5,"R_COMPTEUR_TYPEID":2,"R_FREQUENCEID":null,"R_OPERATIONID":20,"R_MODE_REGLEMENTID":1,"R_TRANSACTION_ETATID":1}
{"ID":3,"R_COMPTEUR_TYPEID":1,"R_FREQUENCEID":2,"R_OPERATIONID":20,"R_MODE_REGLEMENTID":1,"R_TRANSACTION_ETATID":1}
```
```sql
SELECT ID,
       COUNT(*) AS NB_VERSIONS,
       LAST_VALUE(R_COMPTEUR_TYPEID) AS R_COMPTEUR_TYPEID,
       LAST_VALUE(R_OPERATIONID) AS R_OPERATIONID,
       LAST_VALUE(R_MODE_REGLEMENTID) AS R_MODE_REGLEMENTID,
       LAST_VALUE(R_TRANSACTION_ETATID) AS R_TRANSACTION_ETATID
FROM "va.peage.backoffice.socleET.ref_compteur"
WHERE R_COMPTEUR_TYPEID IS NOT NULL AND R_FREQUENCEID IS NOT NULL AND R_OPERATIONID IS NOT NULL
  AND R_MODE_REGLEMENTID IS NOT NULL AND R_TRANSACTION_ETATID IS NOT NULL
GROUP BY ID;
```
Quatre compteurs : l'ID 3 a deux versions et garde la dernière, l'ID 5 est incomplet et filtré.

| ID | NB_VERSIONS | R_COMPTEUR_TYPEID | R_OPERATIONID | R_MODE_REGLEMENTID | R_TRANSACTION_ETATID |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 10 | 1 | 1 |
| 2 | 1 | 2 | 10 | 256 | 1 |
| 3 | 2 | 1 | 20 | 1 | 1 |
| 4 | 1 | 2 | 20 | 512 | 1 |

Les transactions jointes à ce référentiel (`VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR`). Un
compteur de comptage vaut 1, un compteur de somme le montant ; le mode 1 accepte tous les modes.
Joindre le topic brut au lieu du référentiel groupé apparierait chaque transaction à *chaque*
version du compteur 3. Une différence demeure, et c'est elle qui fait de ce lot un job DataStream
dans le laboratoire : une jointure Flink ordinaire réémet les transactions passées quand le
référentiel change, là où ksqlDB joint chaque transaction au référentiel tel qu'il était à son
arrivée. Trois des 48 transactions (`DATE_OPERATION` dépend de l'heure du seed) :
```json
{"R_COMPTEURID":1,"R_MODE_REGLEMENTID":256,"R_OPERATIONID":30,"R_VOIEID":1,"R_GAREID":25004,"R_SOCIETEID":25004,"R_PARTENAIREID":1,"R_TRANSACTION_ETATID":2,"R_TRANSACTION_MODIFICATION_TYPEID":0,"IDT_TRAN_PEA":"TRN-00000","MONTANT_TRANSACTION":2.50,"DATE_OPERATION":1793088407000,"DATE_COMPTABLE":1793088407000}
{"R_COMPTEURID":2,"R_MODE_REGLEMENTID":512,"R_OPERATIONID":10,"R_VOIEID":2,"R_GAREID":25006,"R_SOCIETEID":25006,"R_PARTENAIREID":1,"R_TRANSACTION_ETATID":1,"R_TRANSACTION_MODIFICATION_TYPEID":0,"IDT_TRAN_PEA":"TRN-00001","MONTANT_TRANSACTION":4.61,"DATE_OPERATION":1793088707000,"DATE_COMPTABLE":1793088707000}
{"R_COMPTEURID":3,"R_MODE_REGLEMENTID":1024,"R_OPERATIONID":20,"R_VOIEID":3,"R_GAREID":25021,"R_SOCIETEID":25021,"R_PARTENAIREID":1,"R_TRANSACTION_ETATID":1,"R_TRANSACTION_MODIFICATION_TYPEID":0,"IDT_TRAN_PEA":"TRN-00002","MONTANT_TRANSACTION":6.72,"DATE_OPERATION":1793089007000,"DATE_COMPTABLE":1793089007000}
```
`TRN-00000` porte une opération qu'aucun compteur ne déclare, `TRN-00001` paie en mode 512 là où
le compteur 2 n'accepte que 256, `TRN-00002` correspond au compteur 3 dans sa version mise à jour.
```sql
WITH compteurs AS (
  SELECT ID,
         LAST_VALUE(R_COMPTEUR_TYPEID) AS R_COMPTEUR_TYPEID,
         LAST_VALUE(R_OPERATIONID) AS R_OPERATIONID,
         LAST_VALUE(R_MODE_REGLEMENTID) AS R_MODE_REGLEMENTID,
         LAST_VALUE(R_TRANSACTION_ETATID) AS R_TRANSACTION_ETATID
  FROM "va.peage.backoffice.socleET.ref_compteur"
  WHERE R_COMPTEUR_TYPEID IS NOT NULL AND R_FREQUENCEID IS NOT NULL AND R_OPERATIONID IS NOT NULL
    AND R_MODE_REGLEMENTID IS NOT NULL AND R_TRANSACTION_ETATID IS NOT NULL
  GROUP BY ID
)
SELECT t.IDT_TRAN_PEA, t.R_GAREID, t.R_MODE_REGLEMENTID, c.ID AS R_COMPTEURID,
       CASE WHEN c.R_COMPTEUR_TYPEID = 1 THEN CAST(1 AS DOUBLE) ELSE t.MONTANT_TRANSACTION END AS VALEUR
FROM "va.peage.backoffice.socleET.etancheite" t
JOIN compteurs c ON t.R_COMPTEURID = c.ID
WHERE t.R_OPERATIONID = c.R_OPERATIONID
  AND t.R_TRANSACTION_ETATID = c.R_TRANSACTION_ETATID
  AND (c.R_MODE_REGLEMENTID = 1 OR t.R_MODE_REGLEMENTID = c.R_MODE_REGLEMENTID);
```
Seize lignes, dont :

| IDT_TRAN_PEA | R_GAREID | R_MODE_REGLEMENTID | R_COMPTEURID | VALEUR |
|---|---|---|---|---|
| TRN-00002 | 25021 | 1024 | 3 | 1.0 |
| TRN-00005 | 25006 | 1024 | 1 | 1.0 |
| TRN-00010 | 25021 | 512 | 1 | 1.0 |
| TRN-00013 | 25006 | 512 | 4 | 29.93 |
| TRN-00043 | 25005 | 512 | 4 | 3.23 |
| … | | | | |

## Lot 7 — indicateur horaire

Origine : [`sql/ksqldb/07-08-hourly.sql`](../sql/ksqldb/07-08-hourly.sql) ; migré dans
`lab.WindowJob` ([`sql/flink/07-08-windows.json`](../sql/flink/07-08-windows.json)).

`WINDOW TUMBLING (SIZE 1 HOURS)` devient la fonction de table `TUMBLE`. Kafka Explorer la confie à
son lecteur direct, qui découpe sur `DATE_OPERATION`, l'instant de la transaction en millisecondes
epoch : quatre ou cinq tranches, les quatre heures du seed tombant rarement sur une heure ronde.
Il ne groupe ni par compteur ni par gare comme l'original : il faudrait pour cela une table
déclarée avec un watermark.
```sql
SELECT window_start, window_end,
       COUNT(*) AS NB_TRANSACTIONS,
       SUM(MONTANT_TRANSACTION) AS VALEUR
FROM TABLE(TUMBLE(TABLE "va.peage.backoffice.socleET.etancheite", DESCRIPTOR(DATE_OPERATION), INTERVAL '1' HOUR))
GROUP BY window_start, window_end;
```
Avec un seed lancé à 12 h 10 UTC :

| window_start | window_end | NB_TRANSACTIONS | VALEUR |
|---|---|---|---|
| 2026-10-27 08:00:00 | 2026-10-27 09:00:00 | 11 | 143.55 |
| 2026-10-27 09:00:00 | 2026-10-27 10:00:00 | 12 | 207.78 |
| 2026-10-27 10:00:00 | 2026-10-27 11:00:00 | 12 | 211.62 |
| 2026-10-27 11:00:00 | 2026-10-27 12:00:00 | 12 | 215.46 |
| 2026-10-27 12:00:00 | 2026-10-27 13:00:00 | 1 | 11.67 |

## Lot 8 — qualité de classification

Origine : [`sql/ksqldb/07-08-quality.sql`](../sql/ksqldb/07-08-quality.sql) ; migré dans
`lab.WindowJob`.

Les voies où plus de 30 % d'au moins 20 passages tarifés ont été classés manuellement ou par FC.
La `WINDOW SESSION (60 MINUTES)` de l'original est omise : le lecteur direct l'approcherait par une
fenêtre fixe, et les flux du seed tiennent de toute façon dans une seule session. Un flux, réduit
aux champs que lit la requête (le message porte aussi `BUSINESSFILTER`, `BUSINESSRULE`,
`BUSINESSCYCLOPE`, `RESPONSE` et `TECHNICALFILTER`) :
```json
{
  "FLOW": {
    "STATION": "25004", "LANE": "12", "CREATIONDATE": "2026-10-25T09:00:00.000000Z", "CALLERMODE": "VA",
    "CLASS": [{"CLASSTYPE": "TarifiedClass", "CLASSORIGIN": "Manual"},
              {"CLASSTYPE": "DetectedClass", "CLASSORIGIN": "Auto"}]
  },
  "STATIONLANE": "25004-12"
}
```
| Voie | Flux retenus | Manual ou FC | Ratio | Issue |
|---|---|---|---|---|
| `25004-12` | 25 | 15 | 0.60 | Alarme |
| `25006-03` | 25 (+ 4 `TD`, 4 `PostDTO` exclus) | 4 | 0.16 | Sous le seuil |
| `25021-07` | 12 | 12 | 1.00 | Moins de 20 passages |
```sql
WITH flows AS (
  SELECT STATIONLANE,
         CASE WHEN JSON_QUERY(FLOW, '$.CLASS[*].CLASSORIGIN') LIKE '%"Manual"%'
                OR JSON_QUERY(FLOW, '$.CLASS[*].CLASSORIGIN') LIKE '%"FC"%'
              THEN 1 ELSE 0 END AS WRONG_ORIGIN
  FROM "VA_PEAGE_CLASSIFICATION_FLOWS_EXTEND"
  WHERE JSON_QUERY(FLOW, '$.CLASS[*].CLASSTYPE') LIKE '%"TarifiedClass"%'
    AND JSON_QUERY(FLOW, '$.CLASS[*].CLASSTYPE') NOT LIKE '%"PostDTO"%'
    AND JSON_VALUE(FLOW, '$.CALLERMODE') <> 'TD'
)
SELECT STATIONLANE,
       COUNT(*) AS NBPASSAGE,
       SUM(WRONG_ORIGIN) AS NBCLASSIFWRONGORIGIN,
       SUM(WRONG_ORIGIN) / CAST(COUNT(*) AS DOUBLE) AS RATIO
FROM flows
GROUP BY STATIONLANE
HAVING SUM(WRONG_ORIGIN) / CAST(COUNT(*) AS DOUBLE) > 0.3 AND COUNT(*) >= 20;
```
| STATIONLANE | NBPASSAGE | NBCLASSIFWRONGORIGIN | RATIO |
|---|---|---|---|
| 25004-12 | 25 | 15 | 0.6 |

## Vérification

Les résultats ci-dessus ont été obtenus sur le planner Flink 2.3 de Kafka Explorer à partir des
données du script (la fenêtre du lot 7 sur son lecteur direct). Le workflow `Migration
comparisons` exécute le script contre le broker du laboratoire et vérifie le nombre de messages
de chaque topic.
