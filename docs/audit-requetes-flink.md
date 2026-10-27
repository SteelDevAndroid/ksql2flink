# Audit des requêtes Flink générées

Date : 27 octobre 2026. Périmètre : les 37 traitements migrés (lots 1 à 8),
`sql/flink/*`, générateurs `scripts/generate_*.py` et jobs `lab.SocleEtJob` / `lab.WindowJob`.
Méthode : relecture comparée avec `sql/ksqldb/*`, puis rejeu des sept tests
différentiels du laboratoire (Kafka / ksqlDB 7.9.2 / Flink 1.20.3) après chaque modification.

## 1. Synthèse

| Axe | Constat | Verdict |
| --- | --- | --- |
| Couverture | 37/37 traitements implémentés, traçabilité requête par requête | Conforme |
| Équivalence | Valeurs, clés, timestamps et multiplicités comparés à ksqlDB par des tests automatiques | Conforme sur les jeux testés |
| Garantie de livraison | Sinks `at-least-once` ; le journal ksqlDB ne surcharge pas `processing.guarantee` (défaut `at_least_once`) | Iso, pas de régression |
| Lisibilité SQL (lots 1 à 4) | Sources, vues et branches `UNION ALL` dupliquées par réseau | **Corrigé** (section 2) |
| Topologie lots 3/4 | Le job relisait dans Kafka son propre topic intermédiaire `exploded` | **Corrigé** (section 2) |
| Coût CPU JSON | Données en `STRING` brut, plusieurs analyses JSON par champ et par ligne | Acceptable, à mesurer en charge (section 3) |
| Jobs DataStream (lots 6 à 8) | Scans d'état O(n) par événement, nombre de partitions codé en dur, état non purgé sur clé inactive | À traiter avant la production (section 4) |

## 2. Simplifications appliquées

Toutes ont été validées par les tests différentiels existants, sans modification des tests ni des fixtures.

### 2.1 Lot 1 Lexhistoric : une source multi-topics au lieu de cinq pipelines

Les cinq requêtes ksqlDB partagent schéma, transformation et topic de sortie. Le SQL généré
déclarait cinq tables, cinq vues identiques et un `UNION ALL` à cinq branches qui recopiait cinq fois le `CASE`.

```sql
CREATE TABLE passagesortieva_input (
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector'='kafka',
  'topic'='asf.peage.voie.passagesortieva;esc.peage.voie.passagesortieva;ars.peage.voie.passagesortieva;acr.peage.voie.passagesortieva;cof.peage.voie.passagesortieva',
  ...
);
```

Résultat : 147 → 48 lignes, une seule règle métier à maintenir et un seul consumer group.
L'ordre par partition est conservé, ce qui correspond au seul ordre garanti par ksqlDB.
Test : `PASS: 90 inputs / 40 outputs per engine`.

### 2.2 Lot 2 Conformité : neuf sources réduites à deux

Le même principe s'applique dans le générateur `generate_conformite_sql.py` : une source
multi-topics par sens (entrée : 4 topics, ARS toujours exclu ; sortie : 5 topics) et un `INSERT` par sens.
Résultat : 142 → 51 lignes. Test : `PASS: 100 inputs, 40 entree / 50 sortie outputs per engine`.

### 2.3 Lots 3/4 : suppression de l'aller-retour Kafka du topic `exploded`

Avant, Flink écrivait `*.comptage.bol.exploded` puis le relisait avec une seconde source
pour calculer `*.comptage.bol`. Cet aller-retour reproduisait la topologie ksqlDB, mais n'apporte rien dans un même job :

- double sérialisation/désérialisation et latence d'un aller-retour broker ;
- deux sources et deux consumer groups de plus ;
- exactitude dépendante d'un topic que le job ne contrôle pas (rétention, ACL, producteurs tiers).

Désormais une vue `{net}_exploded` (UNNEST) alimente à la fois le topic `exploded`, toujours
publié pour ses consommateurs, et la projection `bol`. Le planificateur réutilise ce sous-plan
dans le `STATEMENT SET`. Les timestamps sont identiques (hérités de la source).
Test : `PASS: 104 inputs, ten queries, 124 outputs per engine`.

### 2.4 Non-régression

| Test | Résultat |
| --- | --- |
| `smoke.py` | PASS |
| `test_lexhistoric.py` | PASS |
| `test_conformite.py` | PASS |
| `test_lots34.py` | PASS |
| `test_badges.py`, `test_socleet.py`, `test_windows.py` | PASS (code SQL/Java non modifié, rejoué par précaution) |

Les générateurs restent la source de vérité : la CI vérifie que `sql/` est reproductible (`git diff --exit-code`).

## 3. Points relevés sans modification (lots 1 à 5)

| Point | Analyse | Recommandation |
| --- | --- | --- |
| JSON lu en `STRING` puis `JSON_VALUE` répétés | Imposé par la recherche de champs **insensible à la casse** de ksqlDB : le format `json` de Flink est sensible à la casse et casserait les variantes testées | Conserver tant que les producteurs envoient des casses variables. Une fois les producteurs normalisés : `value.format='json'` avec schéma `ROW` typé, ce qui supprime `KSQL_JSON`/`KSQL_PROJECT` et les analyses multiples |
| FAV : ~30 appels `JSON_CONTAINS_STRUCT(JSON_QUERY(...))` par ligne | Chaque appel réanalyse le tableau `ETATEQUIPEMENTS` | Avec un schéma typé : `ARRAY_CONTAINS(ETATEQUIPEMENTS, ROW('comLS','comKO'))`, natif et sans UDF |
| Badges : 4 `INSERT`, 2 `UNNEST` par sens | La fusion CARDME/CIP exigerait des prédicats sentinelles par type : plus complexe pour un gain marginal | Non retenu |
| Options Kafka répétées dans chaque `CREATE TABLE` | Code généré, donc peu gênant | En production : `CREATE TABLE ... LIKE kafka_template (OVERWRITING OPTIONS)` ou un catalogue, pour centraliser bootstrap, sécurité SASL/TLS et groupes |
| `scan.startup.mode='earliest-offset'` | Adapté au laboratoire. En bascule, rejouer depuis le début duplique toute la rétention dans les topics de sortie | À la bascule : `group-offsets` initialisés sur les offsets ksqlDB, ou `timestamp` (voir `conseils-migration.md`) |
| Compatibilité des savepoints SQL | Les UID des opérateurs SQL dérivent du plan : toute modification SQL peut rendre l'état de démarrage inutilisable | Figer les jobs avec `COMPILE PLAN` / `EXECUTE PLAN` ; nommer chaque job (`SET 'pipeline.name'`) |
| `at-least-once` | Iso avec ksqlDB | `exactly-once` possible (`sink.delivery-guarantee`, `transactional-id-prefix`) si les consommateurs lisent en `read_committed` : c'est un gain, pas un prérequis |

> Les modifications 2.1 à 2.3 changent la topologie des sources. Elles doivent donc être
> déployées **avant** la première mise en production : un savepoint pris sur l'ancienne
> topologie ne restaurerait pas les offsets.

## 4. Jobs DataStream (lots 6 à 8) : risques avant production

Le choix de DataStream est justifié : Flink SQL ne reproduit ni l'émission par événement
(`EMIT CHANGES`) des fenêtres ksqlDB, ni les tombstones de fusion de sessions, ni une jointure
stream-table sans recalcul rétroactif en temps de traitement. Les points suivants sont
signalés par un commentaire dans `WindowJob.java` :

| Risque | Emplacement | Impact | Correction proposée |
| --- | --- | --- | --- |
| Nombre de partitions codé en dur (`% 6`) | `WindowJob.partition` | Le temps de flux par partition (sémantique ksqlDB) devient faux si les topics de production n'ont pas 6 partitions | Lire le nombre de partitions depuis la configuration, ou depuis l'admin Kafka au démarrage |
| Scan complet de l'état + analyse JSON à chaque événement | `Hourly`, `Sessions` | Coût O(fenêtres ouvertes de la partition) par message ; parallélisme plafonné au nombre de partitions | Indexer l'état par clé métier, purger par timers event-time, état typé (POJO) au lieu de JSON `String` |
| État non purgé pour une clé inactive | `AlarmJoin` | Croissance non bornée de l'état (fuite) avec la cardinalité gare/voie | Timer de nettoyage à `timestamp + GRACE`, ou `StateTtlConfig` |
| Réécriture complète de `ListState` à chaque événement | `AlarmJoin` | Coût O(n) en RocksDB | `MapState` indexé par timestamp |

Ces corrections modifient la sémantique d'état. Elles doivent être faites avec les tests
`test_windows.py` / `test_socleet.py` et un test de charge, et ne sont donc pas incluses dans cette passe.

## 5. Priorités

1. **Avant la préproduction** : rendre le nombre de partitions paramétrable, purger l'état d'`AlarmJoin`.
2. **Avant la production** : `COMPILE PLAN` pour les jobs SQL, offsets de démarrage alignés sur ksqlDB, test de charge des lots 7/8.
3. **Après la stabilisation** : normaliser la casse côté producteurs, puis passer en schémas typés
   (fin des UDF de compatibilité JSON) ; envisager `exactly-once`.
