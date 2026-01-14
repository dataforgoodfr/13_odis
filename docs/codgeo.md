# Codgeos INSEE

Les codgeo de l'INSEE évoluent tous les ans. En particulier, lorsque des communes sont fusionnées ou scindées, leur codgeo change.
L'INSEE met à disposition une table de passage. Elle permet de mettre à jour les codgeo vers la nomenclature la plus récente, pour tout millésime de codgeo. 

## Sources et domaines bénéficiants de la table de passage codgeo

La source population Nombre de ménage a potentiellement été faite avec un ancien codgeo. On applique la table de passage sur les tables liées à cette source. 
D'autres tables pourraient utiliser cette table de passage. Il serait util de faire un inventaire et d'implémenter les changements nécessaires.

# Tests

On vérifie que les codgeo soient identiques entre silver_population_menages et com_dep_reg. Ainsi, si des codgeo apparaissent ou disparaissent lorsque les codgeo sont mis à jour, il y aura une erreur. Le test exclu les COM et DROM, il semble que les données ménages ne couvrent pas ces territoires. Ce test est a appliqué à toute table utilisant la table de passage INSEE.

Un second test permet de vérifier que la table de passage fonctionne avec son année nominale. Par exemple, la table de passage 2025 fonctionne sur les codgeo 2025: c'est le cas.