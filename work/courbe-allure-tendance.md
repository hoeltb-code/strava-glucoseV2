# Courbe de tendance allure / pente — 20 septembre 2026

Le raccord interpolé entre les anciennes valeurs conservait leurs bosses, notamment après mélange de données personnelles et de médianes collectives. La référence est désormais ajustée globalement après cette sélection.

Formule en secondes/km : `allure = minimum + coefficient × ((pente − optimum) / 30)²`, avec un coefficient pour chaque côté de l’optimum. Coefficients positifs ou nuls, tangente horizontale commune : pas de bosses secondaires. L’optimum est recherché entre −25 % et 0 % par pas de 0,5 point ; cette borne est un choix de modélisation, pas une mesure physiologique. L’erreur relative et une pondération robuste réduisent le poids des points isolés atypiques. Le volume personnel influence le poids avec un plafond.

Les seuils de sélection restent 5 minutes et 20 points personnels par pente/zone, sinon médiane d’au moins 8 autres coureurs. Le modèle exige trois pentes distinctes couvrant au moins 10 points de pourcentage. À défaut, les quelques repères disponibles restent affichés ; les données manquantes ne sont pas remplacées par un profil inventé. Aucune extrapolation au-delà des pentes couvertes n’est exposée par la formule.

Le serveur fournit à la fois les coefficients et leurs valeurs aux centres des tranches. Profil et plan dessinent la formule directement ; cartes, projections GPX et calculs du plan utilisent ses valeurs par tranche. Le chrono cible et l’adaptation au parcours multiplient toute la formule uniformément, après prise en compte des pauses. Les allures observées restent consultables séparément. Les réglages manuels conservent leurs valeurs.

Aucun changement de navigation ni de structure. Les chronos automatiques peuvent changer, puisque la tendance remplace les anciennes valeurs irrégulières.

Validation : 61 tests Python et 6 tests JavaScript ; récupération d’une courbe connue, suppression des extrema parasites après mélange personnel/collectif, données insuffisantes, cohérence API/formule, conservation du chrono cible et du facteur d’échelle, rendu du plan avec la formule. Contrôle visuel dans Safari sur données fictives irrégulières, dans le graphique existant du profil.

Présentation complémentaire : barres arrondies par tranche (bleu descente, vert roulant, orange montée), affichées par défaut dans le même encadré. Le choix « Barres / Courbe » conserve la tendance arrondie. Les deux vues et les trois repères au-dessus utilisent les mêmes allures calculées et suivent la zone cardio sélectionnée. L’axe des barres part de zéro, en minutes/km ; les valeurs manquantes restent absentes. Le test JavaScript supplémentaire vérifie cette cohérence lors des changements de vue et de zone.
