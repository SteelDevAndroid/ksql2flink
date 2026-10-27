# Lots 5 et 6 : contrats de migration

Quatre INSERT d'historiques badges et quatre traitements Socle ET (deux referentiels, deux jointures).
Le journal reste inchange ; `scripts/generate_lots56.py` extrait les SQL de reference et les contrats Kafka.
Les sources de badges sont les topics consolides existants du lot 2, avec les schemas de valeur correspondants.

## Lot 5

Le SQL Flink filtre les memes gares et identifiants d'evenement, puis eclate chaque evenement une seule fois.
PAN, histo et D_VET proviennent ainsi du meme evenement ; plusieurs UNNEST independants seraient incorrects.
Les trois EXPLODE de chaque requete ksqlDB travaillent sur le meme tableau filtre, et sont correles par position.
Les listes de gares et les PAN sentinelles sont extraits des requetes originales (les autres chaines de zeros restent valides).

Les fonctions `BadgeHistory` traduisent les deux lambdas TRANSFORM : formats de dates par concatenation,
gare CARDME construite depuis serviceProvider/locationOfStation, padding CIP, sens 0 -> E sinon S,
montants divises par 100, conservation de l'ordre et des repetitions. Aucune conversion de fuseau horaire n'est ajoutee.
Les tableaux historiques nulls et vides restent distincts. Les sorties entree/sortie partagent les deux topics
historiques comme dans ksqlDB, avec suffixe `.flink` pour la comparaison. Les cles restent nulles.

### Simplification des filtres du lot 5

Les disjonctions d'egalites sur une meme gare sont regroupees en listes IN. Les quatre
requetes contiennent ainsi six listes IN au lieu de 142 egalites. Les deux chemins de
gare utilises en sortie restent relies par OR ; chaque litteral du journal est conserve.
Le generateur refuse les filtres contenant d'autres operations, telles que AND ou une
comparaison differente, afin de ne pas simplifier silencieusement une future regle.

Le SQL badges passe de 16 493 a 7 114 caracteres et de 166 a 30 occurrences textuelles
de JSON_VALUE. Ce gain concerne la lisibilite et les repetitions du code ; aucun gain
de debit ou de latence n'est revendique sans mesure du plan et de l'execution.
Les quatre INSERT, les explosions correlees, les filtres PAN et les sorties restent
distincts comme auparavant. Les ancres de lignes SQL du mapping sont conservees.

## Lot 6 : choix du traitement a etat

Une jointure SQL Flink sur tables dynamiques ne conserve pas automatiquement le contrat stream-table ksqlDB :
seules les transactions doivent declencher une sortie. Le job DataStream `lab.SocleEtJob` realise donc les quatre
traitements en gardant les topics Kafka entre agregation des referentiels et enrichissement des transactions.

- Les enregistrements de referentiel incomplets sont filtres avant agregation, comme dans le WHERE du journal.
- Les ID nulls ne creent pas de groupe. Les cles d'entree ne remplacent pas le champ ID de la valeur.
- COUNT compte tous les enregistrements acceptes, repetitions comprises ; aucune TTL ne supprime cet historique.
- Les colonnes LATEST_BY_OFFSET correspondent a la derniere arrivee acceptee par cle. Tous ces champs sont non nuls apres le filtre.
- Le timestamp d'une mise a jour agregee est le maximum des timestamps acceptes, independamment de la derniere valeur.
- Les cles de referentiel sont des INTEGER Kafka de quatre octets ; ID n'est pas duplique dans la valeur sans AS_VALUE.
- Les deux jointures maintiennent la derniere valeur materialisee par cle. Une mise a jour de referentiel n'emet pas de transaction.
- Un tombstone dans le topic materialise supprime l'etat de recherche ; les sorties deja produites ne sont pas retractees.
- Le compteur exige operation et etat identiques ; ABS(mode)=1 rend uniquement le mode de paiement non discriminant.
- L'indicateur exige uniquement l'existence du referentiel. ABS(type)=1 produit 1.0, sinon le montant de transaction.
- Les cles des sorties sont R_COMPTEURID ; les champs de cle ne sont pas ajoutes aux valeurs JSON.
- Les timestamps des sorties de jointure restent ceux des transactions, pas ceux du referentiel.

## Ordre et portee de l'equivalence

Chaque ID de referentiel doit conserver son ordre dans une partition. Les offsets de partitions differentes ne sont
pas comparables ; ils ne sont pas utilises comme un ordre global. Le resultat de transactions concurrentes a une
mise a jour depend du moment ou chaque moteur traite cette mise a jour. ksqlDB fournit lui-meme une synchronisation
best effort pour ces jointures : l'identite de tous les entrelacements concurrents n'est pas certifiee ici.

Le test ordonne explicitement les phases en attendant les offsets committes par les producteurs d'agregats,
puis par les lecteurs de tables et les consommateurs de transactions des deux moteurs. Il exige deux observations
successives sans retard. Les checkpoints Flink servent de barriere de traitement, pas une attente arbitraire.
Les observations d'offsets sont conservees dans les artefacts du test.

Le jeu couvre les transactions avant chargement, les referentiels incomplets, un doublon de referentiel,
une mise a jour avec timestamp plus ancien, les modes discriminants/generiques, les nulls, une mise a jour sans
recalcul retroactif et une suppression du referentiel materialise. Les quatre flux de sortie sont compares,
y compris les mises a jour intermediaires des agregats et leurs timestamps.

## Execution manuelle

Provisionner les topics de `docs/lots56-contracts.json` : six partitions, cleanup.policy=compact pour les tables
de referentiel, delete pour les autres topics. Les proprietes effectives de retention et replication doivent etre
relevees en production ; le laboratoire utilise un broker et des topics isoles.

```powershell
docker compose run --rm -T sql-client embedded -f /opt/flink/sql/05-badges.sql
docker compose exec -T jobmanager /opt/flink/bin/flink run -d -c lab.SocleEtJob /opt/flink/lib/migration-lab.jar --config /opt/flink/sql/06-socleet.json
```

Pour le lot 6, les deux tables et les deux sorties de la configuration portent le suffixe `.flink`.
Les groupes et UID d'operateurs sont stables pour permettre une restauration explicite depuis un savepoint.
Le demarrage par defaut est earliest : ne pas reconstruire un referentiel depuis un historique incomplet ou deja expire.
La restauration et la bascule necessitent des offsets/etats valides, pas le partage de groupes de consommation ksqlDB.

## Limites

Domaine valide : JSON conformes aux schemas, cles et types attendus, nombres finis, ordre par ID preserve.
Les messages invalides, coercitions hors schema, headers, exactement-une-fois, restauration apres panne et performances
restent a valider avant production. Les sinks sont at-least-once. Les caches/commits ksqlDB du laboratoire peuvent
emettre plus de mises a jour qu'une production avec cache ; la cadence de production doit etre caracterisee separement.
La reprise d'un etat historique non reconstructible n'est pas automatisee. Aucun deploiement de production n'est realise.

## Preuves locales

- Lot 5, execution `badge3925a0fd` : 31 entrees, 35 sorties attendues par moteur ; valeurs, cles,
  timestamps et multiplicites identiques sur les deux topics. Historiques et D_VET distincts par evenement,
  tableaux nuls/vides, elements nuls et identifiants exclus inclus.
- Lot 6, execution `socle1cc25dec` : 26 sorties attendues par moteur dans les quatre topics, dont
  toutes les mises a jour de referentiels et les tombstones. Les traces d'offsets documentent les barrieres
  entre les phases avant chargement, chargement, mise a jour, suppression et nouvelles transactions.

Les rapports JSON et SQL effectifs restent sous `artifacts/<execution>/` et sont publies par la CI comme artefacts.
Ces preuves couvrent les scenarios decrits, pas tous les entrelacements concurrents possibles.

Apres simplification des filtres du lot 5, execution `badge9583cd53` : 31 entrees,
35 sorties identiques par moteur, cles, timestamps, valeurs et multiplicites compris.
Rapport : `artifacts/badge9583cd53/comparison.json`. Les chemins et litteraux des gares
ont aussi ete compares a ceux du journal pour les quatre requetes.
