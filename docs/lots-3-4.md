# Lots 3 et 4

Dix traitements : extension des flux de classification, projection compteur, deux comptages coffre,
deux EXPLODE de bols, deux projections de bols et deux synoptiques FAV (ASF/ESC).

## Traduction et dependances

Les sources et huit schemas JSON utiles sont extraits du journal par `scripts/generate_lots34_sql.py`
(huit schemas sources, dont le contrat compteur). La reference conserve les dix SELECT originaux.
La source compteur est declaree a partir des champs du DESCRIBE de
`VA_PEAGE_BACKOFFICE_SOCLEET_FILTER_REF_COMPTEUR` ; cette declaration ne remplace pas la jointure du lot 6.
Les autres declarations sont extraites directement du journal. Le lanceur isole noms, topics et replication locale.

Le SQL Flink utilise les fonctions SQL natives pour les projections, filtres, CASE, CONCAT, SPLIT_INDEX et UNNEST.
Des fonctions Java de compatibilite assurent la serialisation des schemas, les fragments JSON imbriques,
les elements de tableau JSON et le decodage de la cle Kafka INTEGER. Elles ne reimplementent pas les regles metier.
Les noms quotes et non quotes suivent le comportement valide dans le lot 2.

| Famille | Contrat a conserver |
| --- | --- |
| FLOWS_EXTEND | Tous les champs declares, plus CONCAT(STATION, LANE), sans separateur ; nulls traites comme chaines vides ; collisions conservees |
| Compteur | Cle INTEGER Kafka conservee ; ID reste dans la cle, AS_VALUE copie cette cle dans R_COMPTEURID ; dates restent DOUBLE |
| Coffres | Positions 0->15, 1/piece->7, 1/billet->16, 2->8, 5->14 ; sinon position originale ; type sac/coffre |
| Bols | EXPLODE sans deduplication ; positions 0.1->9, 0.2->10, 0.5->11, 1->12, 2->13 ; sinon CAST INTEGER ; second segment du sujet |
| Variantes comptage | Prefixes de gare 25004 ASF et 25006 ESC ; seuls les bols ESC excluent MRB |
| FAV | Comparaison des structures SUJET/ETAT, priorite des premiers WHEN, null quand un etat est absent |
| Variantes FAV | ASF exclut BECM/VSP mais conserve les types absents ; ESC accepte BE ESCOTA en entree et calcule GAREGROUP sur sa liste de gares |

Toutes les sorties de comparaison sont suffixees `.flink`. Les deux topics exploded sont conserves physiquement
et relus par les projections bol ; aucune fusion de cette frontiere n'est introduite.
Les cles FAV sont des chaines Kafka et restent dans la cle, sans ajout dans la valeur JSON.
Les sources sans colonne KEY n'exportent pas de cle. Les timestamps Kafka sont recopies.
Les tombstones sont propages par les projections du lot 3, mais elimines par EXPLODE et les filtres du lot 4.
Les topics ont six partitions ; retention de sept jours sauf FAV ESC (864000000 ms, dix jours).

## Simplification du SQL genere

Les positions des coffres et bols utilisent desormais la forme simple `CASE valeur WHEN ...`.
Pour les coffres, la position 1 conserve un CASE imbrique sur le type d'espece : piece -> 7,
billet -> 16, sinon 1. Les autres positions, valeurs nulles et le repli TRUNCATE_INTEGER des
bols restent inchanges. Les generateurs restent la source des fichiers SQL ; les references
ksqlDB ne sont pas reecrites.

Le fichier SQL contient ainsi 78 occurrences textuelles de JSON_VALUE au lieu de 96.
Ce chiffre decrit la reduction des repetitions dans le code, pas un gain CPU mesure :
le planificateur peut reecrire les expressions. Les topics et le nombre de lignes SQL
restent inchanges, ce qui conserve les ancres du tableau de mapping.

## Test reproductible

```powershell
docker compose up -d --build --wait
python scripts/test_lots34.py
```

Les fixtures de `fixtures/lots34.json` contiennent les valeurs attendues explicites. La reference independante
reste ksqlDB 7.9.2 avec les requetes du journal. Les sorties intermediaires et finales sont comparees avec leur
multiplicite, les octets des cles et les timestamps. Le transport lit toutes les sorties dans une seule session
par moteur pour ne pas ajouter un delai d'attente par topic. Les rapports restent sous `artifacts/lot34<id>/`.

Le jeu couvre les modes de position, les nombres fractionnaires et negatifs, les sujets sans point et avec point final,
les tableaux vides/nulls, les elements nulls et repetes, les champs absents, les nulls et tombstones, les cles INTEGER
positives/negatives/nulles, les etats FAV contradictoires, les exclusions reseau, les seuils de voie 19/20 et les types ESCOTA.

## Limites et utilisation

Le domaine valide reste celui des schemas declares : JSON valides, types conformes, cles INTEGER de quatre octets
et cles STRING UTF-8 pour les synoptiques. Les casts invalides, les politiques d'erreur sur messages malformes,
les headers, les reprises sur panne, exactly-once et les performances restent a valider avant production.
Les comparaisons n'imposent ni ordre global inter-partitions ni repartition physique identique.

Pour utiliser le SQL manuellement, provisionner les topics de `docs/lots34-contracts.json` avec les proprietes decrites,
ainsi que les dix sorties suffixees `.flink`, puis executer le fichier via le client SQL du laboratoire.
Le demarrage earliest-offset est destine au replay local. La bascule doit fixer les offsets par partition,
arreter les producteurs historiques a une frontiere documentee et eviter deux producteurs sur le meme topic metier.

## Validation locale

Execution `lot347d451791` : 104 messages injectes, 124 sorties attendues par moteur dans dix topics. Les valeurs, cles, timestamps et multiplicites correspondent aux fixtures sur chacun des dix traitements. Le rapport reste dans `artifacts/lot347d451791/comparison.json`.

Apres simplification des CASE, execution `lot3494f9d096` : les 104 entrees et les
124 sorties par moteur passent de nouveau, y compris positions nulles et inconnues,
fractions, doublons et differences ASF/ESC. Rapport : `artifacts/lot3494f9d096/comparison.json`.
