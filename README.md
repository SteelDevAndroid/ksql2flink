# ksql2flink

Laboratoire local Kafka / ksqlDB / Flink SQL pour tester les migrations.
Les requetes historiques du `.log` ne sont pas chargees automatiquement.

## Demarrage

Prerequis : Docker Desktop (conteneurs Linux), Compose v2, environ 8 Go de RAM disponibles
pour Docker et Python 3.10+ pour le test automatise (aucune dependance Python).

```powershell
cd C:\app\GitHub\ksql2flink
docker compose up -d --build --wait
docker compose ps
python scripts/smoke.py
```

Versions : Kafka et ksqlDB Confluent 7.9.2 (version du journal), Flink 1.20.3 Java 17,
connecteur SQL Kafka 3.4.0-1.20. Le build telecharge le connecteur depuis Maven Central.

| Service | Adresse |
| --- | --- |
| Kafka depuis l'hote | `localhost:29092` |
| Kafka depuis Docker | `kafka:9092` |
| API ksqlDB | <http://localhost:8088/info> |
| Interface et API Flink | <http://localhost:8081> |

## Test de comparaison

Le script cree trois topics uniques de six partitions a chaque execution, soumet les deux
variantes SQL et injecte six messages. Il verifie quatre sorties attendues sur chacun des moteurs :
correspondance des modes de reglement Lexhistoric sur un schema simplifie, casse, mode exclu et null.
Les objets JSON sont compares avec leur multiplicite, independamment de l'ordre entre partitions.
Les requetes sont arretees ensuite ; topics et declarations restent disponibles pour inspection.
Rapports : `artifacts/<run-id>/comparison.json` et log de soumission Flink dans le meme dossier.

Ce test valide un filtre/projection et les connexions Kafka. Il ne certifie pas l'equivalence des
requetes historiques : cles, timestamps, tombstones, sessions, grace et reprises restent a tester.
Les sorties de cet exemple utilisent une garantie at-least-once.

## Migration du lot 1 : Lexhistoric

Le [plan de migration](docs/migration-plan.md) classe les 37 traitements persistants en huit lots.
L'[inventaire exhaustif](docs/query-inventory.json) donne le nom, la difficulte et la ligne source de chaque traitement.
Le premier lot migre les cinq projections Lexhistoric (ASF, ESC, ARS, ACR, COF), avec les vrais schemas du journal.

```powershell
docker compose up -d --build --wait
python scripts/test_lexhistoric.py
```

SQL de reference : `sql/ksqldb/02-lexhistoric.sql` ; migration : `sql/flink/02-lexhistoric.sql`.
Le test isole tous les topics, adapte la replication a un broker et compare les valeurs JSON,
leur multiplicite, les cles et les timestamps Kafka. Les fixtures incluent les variantes de casse,
les valeurs nulles, les champs absents, les tableaux vides et plusieurs paiements.
L'image contient une UDF Java pour la recherche de champs JSON sans tenir compte de la casse.
Les cles de sortie sont nulles, conformement aux sources ksqlDB sans colonne KEY.
Les limites de validation et la procedure de bascule sont detaillees dans le plan.

## Migration du lot 2 : passages conformes

Le lot 2 reprend les neuf branches de consolidation (quatre entrees, cinq sorties).
L'entree ARS est declaree dans le journal mais aucune requete ne l'insere dans le flux VA ; elle reste exclue.
Les schemas imbriques complets sont extraits du journal, avec leur casse originale.
Une fonction de serialisation applique ces schemas et distingue les noms quotes des noms non quotes : champs inconnus retires, champs absents materialises a null,
ordre des tableaux conserve. Les messages ne sont pas dedupliques.

```powershell
docker compose up -d --build --wait
python scripts/test_conformite.py
```

SQL : `sql/ksqldb/03-conformite.sql` (reference), `sql/flink/03-conformite.sql` (migration).
Le test injecte 100 messages dans dix entrees et attend 40 sorties entree et 50 sorties sortie par moteur (tombstones compris).
Les donnees couvrent tous les champs declares, les champs inconnus, la casse, les nulls, les tableaux vides,
les doublons exacts, les tombstones et l'exclusion ARS en entree.
Les schemas et SQL sont regenerables avec `python scripts/generate_conformite_sql.py` ; la CI verifie leur reproductibilite.
Le [contrat du lot 2](docs/lot-2-conformite.md) precise les limites et la procedure d'utilisation.

## Consoles SQL

```powershell
docker compose run --rm ksqldb-cli
docker compose run --rm sql-client
```

Les fichiers `sql/ksqldb` sont montes dans `/sql` du CLI ; les fichiers `sql/flink`
dans `/opt/flink/sql`. Exemples d'execution manuelle (creer les topics avant) :

```sql
-- Console ksqlDB
RUN SCRIPT '/sql/01-smoke.sql';
```

```powershell
docker compose run --rm -T sql-client embedded -f /opt/flink/sql/01-smoke.sql
```

Pour les exemples fixes, creer les topics avant de soumettre les deux scripts :

```powershell
foreach ($topic in @('lab.input', 'lab.output.ksql', 'lab.output.flink')) {
  docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 6 --replication-factor 1
}
```

Puis injecter et consulter :

```powershell
Get-Content fixtures/smoke.jsonl | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:9092 --topic lab.input
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic lab.output.flink --from-beginning --timeout-ms 10000
```

Ajouter les migrations dans des paires de fichiers SQL avec des topics de sortie distincts.
Le broker de test est unique : adapter les `REPLICAS=4` historiques a `1`.
Conserver les frontieres Kafka pour les chaines avec etat. Les reglages de cache ksqlDB du
laboratoire sont explicites et ne reproduisent pas necessairement la production.

## Arret et diagnostic

Le workflow GitHub Actions `Migration comparisons` execute les trois tests sur les pull requests,
sur `main` et sur les branches `feat/**`. Les rapports et logs Docker sont conserves sept jours.
Les PR provenant d'un fork peuvent necessiter l'approbation d'un mainteneur pour lancer le workflow.

```powershell
docker compose logs --tail 100 kafka ksqldb jobmanager taskmanager
docker compose down
```

Kafka et les checkpoints Flink utilisent des volumes nommes. Apres redemarrage, resoumettre
les jobs Flink ; leur restauration automatique n'est pas configuree.
Remise a zero volontaire, avec suppression des donnees : `docker compose down -v`.
Ports requis : 29092, 8088, 8081. Services lies a 127.0.0.1, sans authentification, pour tests locaux.
