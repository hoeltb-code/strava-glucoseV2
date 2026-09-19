# Refonte data — réalisation du 19 septembre 2026

Implémentation locale des demandes autorisées. Aucun déploiement ni publication Strava effectué. Les essais utilisent une base SQLite isolée et des données fictives.

## Référence d’allure commune

- Le profil Allures & cardio, les projections GPX et le plan de course utilisent `pace_reference` et le même modèle lissé.
- Une tranche pente × zone cardio est personnelle à partir de 300 secondes exploitables et 20 points. Sinon : médiane des autres coureurs, avec au moins huit personnes par tranche. Chaque personne contribue une seule fois ; l’utilisateur est exclu du collectif.
- La provenance est affichée. En l’absence de modèle personnel et de collectif suffisant, la tranche reste indisponible : aucune médiane inventée. Le réglage manuel demeure un choix explicite.
- Courbe continue avec interpolation qui préserve les points et évite les dépassements. Les projections additionnent les distances par tranche de pente aux allures correspondantes.
- Le chrono cible applique un facteur unique à toute la courbe, après déduction des pauses. Modifier les ravitos recalcule le facteur et les passages. La forme reste identique.

## Énergie

Nouvelle page **Énergie**, avec boutons plat/montée/descente, curseurs de poids, pente, allure, FC, choix course/marche et liaison facultative à la courbe pente × zone cardio.

- Coût du relief : kcal/km. Allure : conversion en kcal/h. La FC situe l’intensité et peut sélectionner la courbe d’allure correspondante.
- Estimation terrain de Minetti ; estimation cardio Keytel séparée, uniquement dans le domaine étudié. Les deux estimations ne s’additionnent pas.
- Activités : dépense estimée, couverture, répartition descente/roulant/montée, pente et FC moyenne, % de FC maximale si connue.
- Plans : estimation selon poids, relief et allures du chrono sélectionné ; répartition par terrain. Approximation par le centre de chaque tranche de pente, course à pied.
- Limites visibles : coût net de locomotion, incertitudes du terrain technique et de la fatigue, marche minimale théorique ; aucune assimilation à une quantité à manger.

Sources : [Minetti, 2002](https://doi.org/10.1152/japplphysiol.01177.2001), [article complet](https://www.skyrunning.com/wp-content/uploads/2020/04/Energy-cost-of-walking-and-running-FSA-works.pdf), [Keytel, 2005](https://pubmed.ncbi.nlm.nih.gov/15966347/), [validation et équations](https://revista-apunts.com/wp-content/uploads/2021/01/Apunts-143-ENG-1.pdf).

## Glycémie et design

- Calcul commun versionné, pondéré par la durée ; 70 et 180 inclus dans la plage. Les longues lacunes sont exclues, sans prolonger les extrémités. Seuil de lacune : 1,5 fois la cadence médiane, borné entre 90 secondes et 15 minutes.
- Détail : glycémie dominante, effort aligné sur le même axe temporel, curseur synchronisé, choix cardio/allure/relief, sélection d’intervalle par champs numériques accessibles, avant 30 minutes / pendant / après 2 heures avec couverture.
- Les données brutes capteur sont prioritaires. Les streams d’activité restent le recours historique lorsque les mesures sources ne sont plus présentes ; leur couverture décrit alors la série archivée.
- Liste : filtres période/sport/glycémie, temps dans la plage, minutes sous 70, couverture, kcal et mini-courbes horodatées avec ruptures.
- Bilan : volume, sorties couvertes, durée observée, énergie, tendance hebdomadaire et comparaison avec une période précédente de même durée. Les archives et le live sont dédupliqués.
- Synthèses compactes préservées dans les contributions d’activité après la rétention des détails à 14 jours. Les anciennes archives sans horodatages ne permettent pas de reconstruire rétroactivement des durées fiables : elles restent sans métrique temporelle.
- Accueil synthétique séparé du plan (`?view=plan`), navigation mobile à cinq entrées, thème clair commun, détails avancés repliables.

## Vitesse et ressources

- Résumés persistés à l’enrichissement ; préparation des anciennes activités en tâche de fond, par petits lots. Les synthèses récentes sont revues au plus une fois par heure pour tenir compte des arrivées CGM tardives.
- Liste sans lecture des streams ; rattrapage du profil sorti des GET ; détail synchrone sorti de la boucle événementielle asynchrone.
- Styles extraits en ressources statiques ; Chart.js chargé une fois par page et version fixé. Leaflet uniquement sur les pages qui l’utilisent. Graphiques avancés différés jusqu’à visibilité ; carte 3D chargée sur demande.
- Une série de profil partagée entre graphiques ; réduction par extrema avec conservation des ruptures. Les statistiques ne sont pas calculées sur les séries réduites.
- Suppression des délais artificiels de génération de plan. Index activité/utilisateur/date et streams/activité/index. Réponses HTML privées non stockables en cache partagé.

Mesure reproductible locale, cinq activités de 721 points : **8 → 3 requêtes SQL** pour la liste, **5 → 0 lectures de streams**. Voir `performance-locale.json`. Cela ne constitue pas une mesure de temps réseau ni un score Lighthouse du site déployé. Compression et Core Web Vitals de production restent à mesurer sur l’hébergement.

## Strava

Deux propositions utilisables dans **Compte → Enrichir Strava** :

1. **Flash** : une ligne kcal estimées + couverture.
2. **Terrain** : montée/roulant/descente, % de pente, allure, FC et % max si connu, kcal.

Aperçus fictifs dans le formulaire. L’option énergie est désactivée par défaut ; l’activation n’envoie aucune mise à jour immédiate. Les autres formulaires préservent ce choix. Le texte personnel reste conservé, même lorsqu’il dépasse le budget de description : seul le contenu généré peut être réduit. Les formats utilisent du texte et des symboles adaptés à la description Strava, sans prétendre afficher du HTML.

## Vérifications

- 55 tests Python (`unittest discover`) et 7 tests JavaScript passent ; cinq fonctions de test de vitesse verticale signée ont aussi été exécutées directement.
- Tests de référence/médiane et seuil de confidentialité, maintien de la forme au chrono cible, changement des pauses, projections GPX, modèles énergie, glycémie temporelle/lacunes/bornes, agrégats pondérés, états vides, pages HTML/JSON et préférences Strava.
- Migration testée deux fois sur un schéma privé antérieur : colonnes et index créés de façon idempotente. Rattrapage testé : pas de double comptage live/archive, maintien de la synthèse après disparition du live.
- Rendu réel dans Chrome vérifié : simulateur énergie sur ordinateur, énergie et activité à 390 px, changement cardio/allure et calcul d’intervalle, courbe collective lissée dans le profil.
- Templates Jinja, syntaxe des scripts et `git diff --check` vérifiés.

## Mise en service

Au prochain démarrage applicatif, `init_db()` ajoute les deux colonnes et les index ; le worker local prépare les résumés manquants. La commande explicite `python -m scripts.backfill_activity_analytics` permet aussi d’effectuer le rattrapage. Ni migration ni rattrapage n’ont été exécutés sur la base réelle pendant cette intervention.
