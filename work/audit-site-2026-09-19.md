# Audit Running Data Plan — 19 septembre 2026

## Périmètre et conclusion

Audit du code FastAPI, des modèles de données et des templates de l’accueil connecté, de la liste des activités, du détail d’activité et du profil coureur. Aucun changement du fonctionnement de l’application ni des données personnelles. La proposition visuelle utilise uniquement des données fictives.

Ce n’est pas une mesure Lighthouse ni une inspection visuelle du site déployé : aucun onglet de navigateur connecté n’était exposé à l’outil. Les constats de code sont vérifiés ; les gains de vitesse restent à mesurer. Les tailles ci-dessous sont celles des fichiers sources, pas les volumes transférés après rendu et compression.

**Direction recommandée : un tableau de bord sportif clair où la glycémie devient une dimension principale de l’analyse.** Conserver FastAPI/Jinja et Chart.js ; améliorer d’abord les calculs, les chargements et la hiérarchie des écrans.

## Ce qui existe déjà et mérite d’être conservé

- Palette claire, cartes blanches, typographie Manrope et chiffres DM Mono.
- Mini-profils SVG d’altitude et de glycémie sur les cartes d’activité.
- Graphiques qui croisent relief, cardio, allure et glycémie dans le détail.
- Vue glycémie sur 24 h, variabilité, répartition par zones et historique des 20 dernières sorties dans le profil.
- Agrégats mensuels et contributions d’activité déjà disponibles pour construire des bilans rapides.
- Pagination de la liste à cinq activités, réduction de certaines séries graphiques, animations désactivées sur plusieurs graphiques.
- Actualisation CGM déjà déportée dans un thread : ne pas la présenter comme un appel externe systématiquement bloquant au chargement.

## 1. Performance : les actions prioritaires

| Priorité | Constat vérifié | Action proposée | Effort relatif |
|---|---|---|---|
| P1 | La liste exécute une lecture des streams par activité puis recalcule D+, D− et quatre fenêtres glissantes à chaque affichage. | Conserver les résumés et mini-séries à l’enrichissement ; lire seulement ces résumés pour la liste. À court terme, regrouper les lectures. | Moyen |
| P1 | Le profil parcourt les activités pour compléter les archives, puis peut migrer six activités et reconstruire un cache pendant un GET. | Sortir le rattrapage des routes de consultation ; utiliser une tâche idempotente et afficher la dernière synthèse disponible avec sa date. | Moyen à élevé |
| P1 | Le détail est une route `async def` contenant des accès SQLAlchemy synchrones et des calculs Python, sans aucun `await`. | Déplacer les lectures synchrones hors de la boucle événementielle ; un handler `def` peut constituer un premier correctif, avec pré-calcul des traitements coûteux. Mesurer sous concurrence. | Moyen |
| P1 | Les différents onglets du détail sont présents dans le HTML ; leurs scripts exécutent les créations de graphiques même s’ils sont masqués. | Initialiser les graphiques au premier affichage de l’onglet ; charger la carte uniquement à la demande. | Moyen |
| P1 | Chart.js apparaît dans six balises du détail, sans version fixée ; le même JSON de profil est injecté à plusieurs endroits. | Un seul chargement versionné et un seul jeu de données partagé. Le cache navigateur peut éviter un transfert répété, mais pas nécessairement la réexécution des scripts. | Faible à moyen |
| P2 | Leaflet est chargé dans le `head` de toutes les pages ; MapLibre est aussi chargé dans le détail. | Charger chaque moteur uniquement sur les vues qui l’utilisent ; choisir un moteur par besoin de carte. Respecter l’ordre des scripts dépendants lors du passage en différé. | Faible à moyen |
| P2 | Le profil relit séparément les points sur 1, 7 et 14 jours, puis de nouveau sur 24 h. Ces traitements sont déclenchés dès qu’un capteur est connecté, même pour un onglet sportif. | Agréger depuis une seule fenêtre pertinente ou des résumés journaliers ; ne calculer que les données de l’onglet demandé. | Moyen |
| P2 | CSS et JavaScript sont principalement intégrés aux templates, avec des surcharges de style successives. | Extraire les composants et ressources par page ; les versionner pour permettre leur cache entre navigations. | Moyen |
| P2 | La réduction du profil utilise `len(points) // 900` : pour 1 799 points, le pas reste 1. | Fixer un véritable budget graphique adapté à la largeur ; préserver extrêmes, franchissements de seuil et interruptions. | Faible à moyen |

Références : `app/main.py:10472` (liste), `app/main.py:6973` (profil), `app/main.py:10646` (détail), `app/main.py:7222` (fenêtres CGM), `app/main.py:11486` (réduction), `templates/base.html:38`, `templates/activity_detail.html:716`, `templates/activity_detail.html:1161`, `templates/activity_detail.html:1320`, `templates/activity_detail.html:1389`, `templates/activity_detail.html:1637`, `templates/activity_detail.html:1733`.

La page source d’accueil connecté fait 296 785 octets, le profil 126 209, le détail 96 282, et le socle commun 28 130. Ces chiffres justifient de mesurer le poids réellement rendu ; ils ne permettent pas de conclure à un temps de chargement précis.

Pour les index, vérifier les plans d’exécution avant migration : candidats `(activity_id, idx)` sur les streams et `(user_id, start_date)` sur les activités. La contrainte unique `(user_id, ts)` des points CGM existe déjà : ne pas ajouter un index identique sans raison. Vérifier aussi la compression et les en-têtes réels côté hébergement : leur absence dans le middleware applicatif ne prouve pas leur absence en production.

Chart.js recommande notamment la préparation des données, la réduction des séries et la limitation des animations : [documentation officielle](https://www.chartjs.org/docs/latest/general/performance.html). Le traitement des fonctions synchrones et asynchrones est décrit dans la [documentation FastAPI](https://fastapi.tiangolo.com/async/).

## 2. Fiabilité de la présentation glycémique

Ces sujets passent avant l’ajout de nouveaux indicateurs.

### Unifier ce que signifie « temps dans la plage »

`app/logic.py:803` calcule des pourcentages par nombre de mesures. Le détail (`app/main.py:10764`) et le profil (`app/main.py:7222`) attribuent des durées entre deux points successifs. Les résultats peuvent diverger lorsque l’échantillonnage est irrégulier.

La borne 180 est également incohérente : `compute_stats` l’inclut dans la plage tandis que la zone haute du détail démarre à 180, malgré son libellé « > 180 ». Centraliser bornes, inclusivité, unités et mode de calcul dans une seule définition versionnée. La mention « personnalisables » du détail doit correspondre à un réglage réellement utilisé ; les limites observées sont codées en dur dans cette route.

### Distinguer durée observée et durée sans mesure

Les boucles de distribution inspectées ajoutent tout l’intervalle entre deux points valides sans plafond de lacune. Une interruption longue peut ainsi être attribuée à la dernière valeur connue. Définir une règle explicite de validité des intervalles, adaptée à la cadence du fournisseur ; exclure les intervalles non couverts du dénominateur et afficher séparément la couverture.

Présenter « temps dans la plage sur la durée observée » avec le nombre de minutes observées, la part manquante, la source et la fraîcheur des données. Ne pas afficher 0 % quand aucune mesure n’est disponible.

### Ne pas masquer les variations

La mini-courbe de liste retire les valeurs absentes, espace uniformément les points et normalise chaque activité entre son minimum et son maximum (`app/main.py:10490`). Deux sorties d’amplitudes très différentes peuvent paraître semblables. Conserver les horodatages, représenter les lacunes et afficher des repères comparables. La réduction visuelle ne doit pas servir au calcul des statistiques.

### Aligner le bilan sur les filtres

`get_cached_glucose_activity_summary` (`app/logic.py:4663`) prend le sport et une limite mais aucune période. Il rassemble les archives, limite les activités à 20, et conserve des statistiques par effort portant sur toutes les lignes lues. Le profil possède pourtant un sélecteur de période. Donner aux graphiques un périmètre explicite et cohérent : période, sport, activités couvertes et volume observé.

### Préférer les observations aux verdicts

Le détail génère des phrases comme « Pense à réduire les apports rapides » à partir d’une proportion élevée et peut appeler le profil « stable » dans le cas restant (`app/main.py:10868`). Remplacer ces raccourcis par des constats descriptifs et vérifiables. Une proportion dans une plage ne mesure pas à elle seule la stabilité ; afficher séparément la variabilité. Les associations effort/glycémie ne suffisent pas à conclure à une cause ni à prescrire des apports.

## 3. Proposition par écran

### Accueil connecté

Premier écran : synthèse de la période, dernière activité et évolution glycémique. Trois ou quatre indicateurs maximum, suivis d’un graphique dominant. La préparation de course reste accessible dans un espace dédié ; son long formulaire et sa logique ne doivent pas dominer chaque retour sur le site.

Navigation proposée : **Vue d’ensemble · Activités · Bilan athlète · Préparer une course**, puis compte et connexions dans un accès secondaire.

### Liste des activités

Sur chaque ligne ou carte : date, sport, nom, distance, durée et D+, puis une courbe glycémique miniature avec repères de plage. Ajouter le pourcentage dans la plage, les minutes sous la borne et la couverture. Le meilleur D+ glissant peut rester dans l’analyse détaillée.

Remplacer les grandes cartes « affichées sur cette page » et « dernière distance » par des informations utiles sur la période : sorties couvertes par le capteur, durée d’effort observée, tendance glycémique. Filtres de période, sport et disponibilité des données ; ordre chronologique explicite.

### Détail d’activité

1. En-tête compact : nom réel de la sortie, date, sport, distance, D+, durée.
2. Synthèse glycémique : départ → arrivée, temps dans la plage, minutes sous la borne ; couverture toujours visible.
3. Courbe de glycémie en fonction du temps, avec plage en fond pâle, seuils libellés et interruptions visibles.
4. Sous cette courbe, un panneau d’effort au même axe temporel : cardio ou allure, puis relief en option. Curseur synchronisé et sélection d’intervalle mettent à jour une petite synthèse locale.
5. Barre de répartition par zones, et analyse par phases : avant, pendant, après. Chaque phase indique sa couverture réelle.
6. Carte, montées, splits et croisements avancés accessibles à la demande.

Afficher plusieurs échelles verticales superposées rendrait la lecture ambiguë : préférer des panneaux alignés. Les événements d’alimentation ne doivent apparaître que si l’athlète les a effectivement renseignés. L’avant/après nécessite une collecte et une conservation suffisantes ; ne pas promettre une récupération rétrospective si le fournisseur ne la permet pas.

### Bilan athlète

Unifier période et sport pour toutes les sections. Proposer une lecture courte de l’évolution :

- Volume d’entraînement et proportion d’activités disposant de mesures.
- Évolution hebdomadaire du temps dans la plage, avec minutes hors plage et volume observé.
- Variabilité et distribution de la glycémie à l’effort ; comparaison avec la période précédente à couverture comparable.
- Comparaison endurance, seuil et fractionné sous forme de distributions ou de points par activité, avec effectif et durée observée. Éviter une moyenne seule qui masque la dispersion.
- Comparaison avant/pendant/après à fenêtres identiques, pour les sorties suffisamment couvertes.
- Historique cliquable des activités à l’origine des résultats.

Une carte thermique « zone cardio × plage glycémique » est utile en second niveau si les cellules affichent leur durée observée. Ne pas multiplier les radars ou fabriquer un score global de santé/performance.

## 4. Direction visuelle claire et orientée données

| Usage | Proposition |
|---|---|
| Fond général | Ivoire très léger `#F7F8F5` |
| Surfaces | Blanc `#FFFFFF`, filets `#DDE4DF` |
| Texte principal | Bleu encre `#17243E` |
| Texte secondaire | Gris ardoise `#596773` |
| Courbe glycémie | Vert profond `#287B65` |
| Plage de référence | Vert très pâle `#EAF4EC` |
| Cardio | Corail `#BD6464` |
| Allure | Bleu `#527EB5` |
| Accent de marque | Ambre `#F8B858`, utilisé avec parcimonie |

Conserver Manrope pour les libellés et des chiffres tabulaires pour les valeurs. Réduire les bandeaux éditoriaux, les ombres et les effets décoratifs ; homogénéiser les marges et rayons. Mettre le contraste sur les courbes et le texte, garder les grandes surfaces douces. Les valeurs doivent avoir une unité, les graphiques des axes et les couleurs une signification constante.

Sur mobile : une seule colonne pour les grands graphiques, synthèses compactes, légendes repliables si nécessaire, contrôles utilisables au toucher, focus clavier visible et alternative textuelle aux graphiques. Vérifier les contrastes réellement calculés, les débordements et la lisibilité à 320/390 px pendant la réalisation.

## 5. Ordre de réalisation et validation

**Lot 1 — fiabilité et vitesse.** Centraliser les métriques CGM ; tester les bornes, les lacunes et les périodes. Retirer les migrations des GET. Pré-calculer les résumés de liste. Charger une seule fois les bibliothèques et initialiser les vues à la demande.

**Lot 2 — activité et liste.** Appliquer les composants visuels communs ; créer la courbe temporelle dominante et les synthèses avec couverture ; simplifier les cartes d’historique.

**Lot 3 — bilan et accueil.** Relier toutes les vues aux mêmes filtres, ajouter les tendances et comparaisons, puis déplacer le préparateur de course vers son espace dédié.

Critères de validation :

- Le même ensemble de mesures donne les mêmes métriques dans l’activité, le bilan et les résumés.
- Les lacunes, un seul point, aucune donnée, une valeur sur une borne et des cadences mixtes sont correctement traités.
- Les filtres de période portent sur les données et les dénominateurs de tous les graphiques concernés.
- La liste utilise un nombre de requêtes borné sans relire les streams complets pour chaque carte.
- Une consultation ne lance pas de migration/reconstruction lourde ; aucun graphique masqué n’est initialisé inutilement.
- Comparer avant/après sur une activité courte, une sortie de plusieurs heures et un historique volumineux : temps SQL, TTFB, poids HTML/JS, temps de calcul des graphiques et mémoire.
- Mesurer mobile et ordinateur, cache froid et chaud, et sous plusieurs consultations simultanées.
- Objectifs d’expérience : LCP ≤ 2,5 s, INP ≤ 200 ms, CLS ≤ 0,1 au 75e percentile. Ce sont des objectifs, pas des scores obtenus : [référence Google Web Vitals](https://web.dev/articles/vitals).

Les bénéfices attendus sont un premier écran plus rapide, moins de calcul répété, des graphiques comparables et un parcours où l’athlète comprend immédiatement ce qui s’est passé pendant sa sortie. Leur ampleur sera établie par les mesures avant/après.
