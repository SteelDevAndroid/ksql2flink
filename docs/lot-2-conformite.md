# Lot 2 : consolidation des passages conformes

Neuf traitements persistants du journal sont migres : deux CSAS depuis ASF, quatre INSERT sortie
(ESC, ARS, ACR, COF) et trois INSERT entree (ESC, ACR, COF). L'entree ARS ne fait pas partie du traitement
actuel, bien qu'une declaration de source existe dans le journal. Ajouter cette branche serait un changement metier.

## Architecture et contrat

Les cinq sources sortie et quatre sources entree sont des topics de six partitions.
Deux INSERT Flink dans un meme STATEMENT SET consolident ces sources avec UNION ALL, sans deduplication.
Les deux sorties de comparaison portent le suffixe `.flink` :

- `va.peage.backoffice.conformite.entree.conforme.flink`
- `va.peage.backoffice.conformite.sortie.conforme.flink`

Ne pas activer simultanement les producteurs ksqlDB et Flink sur les topics historiques.
Les sorties ont six partitions, une retention de sept jours et cleanup.policy=delete.
Le runner cree les topics isoles avec ces proprietes et une replication locale de 1.

La fonction `ProjectKsqlJson` reconstitue la valeur du SELECT * selon le schema declare dans le journal :

- identifiants quotes recherches avec leur casse exacte ; identifiants non quotes recherches sans tenir compte de la casse ;
- noms de sortie exactement conformes au schema, y compris `DHM`, `PAN`, `SUJET` et `EFC-ContextMark` ;
- seuls les champs declares sont emis, y compris dans les structures et tableaux imbriques ;
- champs absents emis a null ; structure nulle conservee a null ; structure presente mais vide materialisee ;
- tableaux conserves dans leur ordre, avec elements nulls et doublons ;
- nombres INTEGER et DOUBLE serialises selon le type declare ; valeurs STRING non modifiees ;
- cles de sortie nulles (aucune colonne KEY dans les declarations originales) ;
- timestamps Kafka recopies via les metadonnees, sans utiliser les dates metier du payload ;
- tombstones propages dans les sorties, conformement au SELECT * ksqlDB observe.

Le domaine valide contient des JSON conformes aux types declares. Les JSON malformes, les types incompatibles,
la coercition de types et les doublons de noms JSON strictement identiques ne sont pas certifies.
La fonction echoue sur une incompatibilite de type ; la politique d'erreur ksqlDB peut etre differente.
Les garanties de panne, les headers Kafka, la repartition physique et l'ordre inter-partitions restent hors du test.
Les sorties sont at-least-once ; la garantie exactly-once doit etre validee avant production.

## Provenance et regeneration

`scripts/generate_conformite_sql.py` extrait les DDL et INSERT originaux depuis le journal inchange.
Il verifie que les schemas des reseaux consolides sont identiques pour chaque direction.
Les identifiants non quotes du journal sont en majuscules ; les identifiants quotes ne le sont pas entierement.
Le generateur refuse un nouveau champ quote entierement en majuscules afin de ne pas confondre ces regles de recherche.
Le parseur est volontairement limite aux types du lot (STRING, INTEGER, DOUBLE, STRUCT, ARRAY) et echoue pour un nouveau type.
Les schemas de serialisation sont embarques dans le JAR depuis `docker/flink/resources/schemas`.

```powershell
python scripts/generate_conformite_sql.py
docker compose up -d --build --wait
python scripts/test_conformite.py
```

Le test utilise les SQL du journal comme reference independante et un jeu de fixtures a resultats attendus explicites.
Chaque execution emploie des noms de topics, objets ksqlDB et identifiants de requete uniques.
Il compare les valeurs completes avec leur multiplicite, les cles et les timestamps, dans les deux sorties.
Un doublon exact (meme valeur, cle, partition et timestamp) doit produire deux sorties.
L'entree ARS recoit les memes fixtures et ne doit produire aucune sortie.
Les requetes sont arretees ensuite ; topics et declarations restent inspectables.
Les rapports, SQL effectifs et donnees injectees sont dans `artifacts/conf<run-id>/`.

## Execution manuelle et bascule

Creer prealablement les neuf entrees et deux sorties du SQL avec six partitions ; retention de sortie 604800000 ms,
cleanup.policy=delete, replication adaptee a l'environnement. Soumettre ensuite :

```powershell
docker compose exec -T jobmanager /opt/flink/bin/flink run -d -c lab.SqlRunner /opt/flink/lib/migration-lab.jar /opt/flink/sql/03-conformite.sql
```

Le SQL utilise earliest-offset pour le replay local. Une bascule de production doit fixer les offsets par partition,
valider les consommateurs des sorties miroirs, arreter les neuf branches ksqlDB a une frontiere documentee puis
activer les ecritures Flink sur les topics historiques aux offsets suivants. Prevoir le retour arriere aux bons offsets.
Aucun deploiement de production n'est realise par cette migration.

## Validation locale

Execution `confd3e6f4d2` : 100 entrees, 40 sorties entree et 50 sorties sortie par moteur,
tombstones compris. Les deux moteurs correspondent aux fixtures attendues pour les valeurs completes,
les cles nulles, les timestamps et la multiplicite. Les tableaux contiennent des elements distincts pour
detecter une inversion d'ordre ; le doublon exact est conserve. Le rapport reste dans
`artifacts/confd3e6f4d2/comparison.json` (ignore par Git).
