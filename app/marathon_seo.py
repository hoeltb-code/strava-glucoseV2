"""Catalogue éditorial des pages SEO consacrées aux marathons.

Les profils restent volontairement qualitatifs : le parcours, le dénivelé et
les ravitaillements peuvent changer d'une édition à l'autre. L'organisateur
reste donc la source de référence avant toute projection.
"""


def _marathon(name, city, country, region, profile, character, official_url, *, major=False):
    slug = name.lower()
    replacements = {
        " ": "-", "’": "-", "'": "-", "é": "e", "è": "e", "ê": "e",
        "à": "a", "â": "a", "ô": "o", "ö": "o", "ü": "u", "ï": "i",
        "ç": "c", "á": "a", "í": "i", "ã": "a", "–": "-",
    }
    for source, target in replacements.items():
        slug = slug.replace(source, target)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return {
        "slug": slug.strip("-"),
        "name": name,
        "city": city,
        "country": country,
        "region": region,
        "distance": "42,195 km",
        "profile": profile,
        "character": character,
        "official_url": official_url,
        "major": major,
    }


WORLD_MARATHONS = [
    _marathon("Marathon de Tokyo", "Tokyo", "Japon", "monde", "Un marathon urbain globalement roulant, où les changements de direction, les ponts et la densité du peloton doivent néanmoins entrer dans le plan d’allure.", "Un grand rendez-vous asiatique au cœur de Tokyo, avec une logistique de départ et une ambiance qui imposent d’anticiper les déplacements.", "https://www.marathon.tokyo/en/", major=True),
    _marathon("Marathon de Boston", "Boston", "États-Unis", "monde", "Un parcours en ligne réputé exigeant : la première partie descendante peut solliciter les quadriceps avant les bosses tardives de Newton et Heartbreak Hill.", "Le plus ancien marathon annuel au monde et une course à qualification, où la patience compte autant que le niveau chronométrique.", "https://www.baa.org/races/boston-marathon/", major=True),
    _marathon("Marathon de Londres", "Londres", "Royaume-Uni", "europe", "Un parcours urbain plutôt roulant, animé par une forte densité de coureurs et quelques variations qui rendent le placement et la régularité importants.", "L’un des deux Abbott World Marathon Majors organisés en Europe, célèbre pour son public et son arrivée au centre de Londres.", "https://www.tcslondonmarathon.com/", major=True),
    _marathon("Marathon de Berlin", "Berlin", "Allemagne", "europe", "Un tracé très roulant associé à la recherche de performance, sans pour autant garantir un chrono : météo, virages, ravitaillements et densité restent déterminants.", "Un Major européen emblématique, connu pour ses performances de très haut niveau et son passage près de la porte de Brandebourg.", "https://www.bmw-berlin-marathon.com/en/", major=True),
    _marathon("Marathon de Chicago", "Chicago", "États-Unis", "monde", "Une boucle urbaine rapide mais exposée au vent et aux variations de température ; les ponts et les longues lignes droites demandent une allure bien verrouillée.", "Un Major nord-américain traversant de nombreux quartiers de Chicago avec un public dense.", "https://www.chicagomarathon.com/", major=True),
    _marathon("Marathon de New York", "New York", "États-Unis", "monde", "Un parcours loin d’être plat, rythmé par les ponts, les relances et une fin exigeante dans Central Park.", "Un Major spectaculaire à travers les cinq boroughs, où l’énergie du public peut pousser à partir trop vite.", "https://www.nyrr.org/tcsnycmarathon", major=True),
    _marathon("Marathon de Sydney", "Sydney", "Australie", "monde", "Un tracé urbain avec des ondulations et des repères iconiques ; la gestion des montées courtes et du climat doit être intégrée au pacing.", "Le grand marathon australien devenu Abbott World Marathon Major, avec une arrivée emblématique à Sydney.", "https://tcssydneymarathon.com/", major=True),
    _marathon("Marathon du Cap", "Le Cap", "Afrique du Sud", "monde", "Un parcours urbain où le vent, la température et les variations du terrain peuvent compter davantage qu’un simple profil moyen.", "Le Sanlam Cape Town Marathon est devenu le huitième Abbott World Marathon Major et une référence majeure sur le continent africain.", "https://capetownmarathon.com/", major=True),
    _marathon("Marathon de Valence", "Valence", "Espagne", "europe", "Un profil rapide et roulant, propice à une stratégie régulière et à un objectif chronométrique précis si les conditions sont favorables.", "Une référence internationale de la course sur route en Espagne, recherchée pour la performance.", "https://www.valenciaciudaddelrunning.com/en/marathon/"),
    _marathon("Marathon de Rotterdam", "Rotterdam", "Pays-Bas", "europe", "Un parcours roulant où le vent et l’allure collective peuvent modifier la dépense énergétique malgré un dénivelé limité.", "Un grand marathon néerlandais réputé rapide, porté par une forte culture populaire de la course.", "https://www.nnmarathonrotterdam.org/"),
    _marathon("Marathon d’Amsterdam", "Amsterdam", "Pays-Bas", "europe", "Un parcours plutôt roulant avec des portions urbaines et plus exposées ; garder un effort stable compte davantage que réagir à chaque variation instantanée.", "Une course internationale avec un départ et une arrivée associés au stade olympique.", "https://www.tcsamsterdammarathon.eu/"),
    _marathon("Marathon de Rome", "Rome", "Italie", "europe", "Un profil urbain irrégulier où les pavés, les relances et les petites variations de pente demandent plus de prudence qu’un parcours parfaitement roulant.", "Un marathon patrimonial traversant le centre historique de Rome.", "https://www.runromethemarathon.com/"),
    _marathon("Marathon d’Athènes", "Athènes", "Grèce", "europe", "Un parcours historique en ligne comprenant une longue séquence montante avant la descente vers Athènes ; l’allure doit être pensée par sections.", "La course dite authentique relie Marathon à Athènes et se termine dans le stade panathénaïque.", "https://www.athensauthenticmarathon.gr/"),
    _marathon("Marathon de Barcelone", "Barcelone", "Espagne", "europe", "Un parcours urbain rythmé par des faux plats et des relances, qui invite à raisonner en effort plutôt qu’à imposer une allure identique partout.", "Un grand marathon méditerranéen au milieu des principaux repères architecturaux de Barcelone.", "https://www.zurichmaratobarcelona.es/en/"),
    _marathon("Marathon de Séville", "Séville", "Espagne", "europe", "Un tracé très roulant où la chaleur éventuelle, l’hydratation et le contrôle du départ restent les principaux pièges d’un objectif ambitieux.", "L’un des grands marathons espagnols pour viser un chrono sur route.", "https://www.zurichmaratonsevilla.es/"),
    _marathon("Marathon de Vienne", "Vienne", "Autriche", "europe", "Un marathon urbain relativement roulant, avec quelques variations et des portions exposées qui justifient une projection sensible au vent et à la météo.", "Une grande course européenne entre larges avenues et monuments viennois.", "https://www.vienna-marathon.com/"),
    _marathon("Marathon de Prague", "Prague", "Tchéquie", "europe", "Un parcours urbain où pavés, ponts, virages et passages étroits peuvent casser la régularité de l’allure.", "Un marathon international au décor historique, à préparer comme une course de rythme et de relances.", "https://www.runczech.com/en/events/orlen-prague-marathon-2026"),
    _marathon("Marathon de Dublin", "Dublin", "Irlande", "europe", "Un parcours ondulé où les variations de pente et la météo irlandaise peuvent peser sur la seconde moitié.", "Une grande course populaire européenne reconnue pour son ambiance et son public.", "https://irishlifedublinmarathon.ie/"),
    _marathon("Marathon de Stockholm", "Stockholm", "Suède", "europe", "Un parcours urbain avec des ponts et des ondulations ; la lecture du profil aide à positionner les accélérations sans subir les relances.", "Une référence scandinave qui combine course sur route et découverte de Stockholm.", "https://www.stockholmmarathon.se/eng/"),
    _marathon("Marathon de Copenhague", "Copenhague", "Danemark", "europe", "Un marathon urbain globalement roulant, où le vent, la densité et les nombreux changements de direction peuvent faire varier l’allure réelle.", "Une grande course scandinave connue pour son public et son parcours au cœur de Copenhague.", "https://copenhagenmarathon.dk/en/"),
]


FRANCE_MARATHONS = [
    _marathon("Marathon de Paris", "Paris", "France", "france", "Un parcours urbain qui comporte davantage de variations qu’un marathon totalement plat, avec des tunnels, des quais et une fin où le dénivelé cumulé se fait sentir.", "Le plus grand marathon français, entre monuments, forte densité de participants et logistique de grand événement.", "https://www.schneiderelectricparismarathon.com/"),
    _marathon("Marathon Nice-Cannes", "Nice et Cannes", "France", "france", "Un parcours côtier en ligne : le profil paraît roulant, mais le vent, l’exposition et quelques variations du littoral doivent être intégrés à la projection.", "Une traversée emblématique de la Côte d’Azur entre Nice et Cannes.", "https://www.marathon06.com/"),
    _marathon("Marathon du Médoc", "Pauillac", "France", "france", "Un parcours festif au milieu des vignobles, avec des chemins et des ondulations qui éloignent la course d’une recherche de chrono parfaitement linéaire.", "Un marathon français mondialement connu pour ses déguisements, ses châteaux et son identité viticole.", "https://www.marathondumedoc.com/"),
    _marathon("Marathon de Lyon", "Lyon", "France", "france", "Un parcours urbain à analyser édition par édition : quais, changements de direction et petites variations peuvent influencer l’allure cible.", "Le format marathon de Run in Lyon, au sein d’un grand événement populaire.", "https://www.runinlyon.com/fr/"),
    _marathon("Marathon de Toulouse", "Toulouse", "France", "france", "Un marathon métropolitain plutôt roulant dont les relances, l’exposition et la météo doivent rester dans le scénario de course.", "Le marathon de la Toulouse Métropole Run Experience, au cœur de la ville rose.", "https://www.marathondetoulousemetropole.fr/"),
    _marathon("Marathon de Nantes", "Nantes", "France", "france", "Un parcours urbain globalement accessible, mais les ponts, virages et changements de revêtement justifient une lecture détaillée du tracé officiel.", "Une grande course de l’Ouest avec un parcours au cœur de Nantes.", "https://www.marathon-nantes.com/"),
    _marathon("Marathon de La Rochelle", "La Rochelle", "France", "france", "Un parcours réputé roulant, où le vent côtier et la régularité des ravitaillements peuvent devenir les facteurs décisifs.", "Une référence historique et populaire du calendrier français de fin de saison.", "https://www.marathon17.fr/"),
    _marathon("Marathon Vert Rennes", "Rennes", "France", "france", "Un tracé à vérifier selon l’édition, avec un profil favorable à une allure régulière mais sensible au vent et aux conditions automnales.", "Un grand rendez-vous breton associé à une démarche environnementale et solidaire.", "https://www.lemarathonvert.org/"),
    _marathon("Marathon de Deauville", "Deauville", "France", "france", "Un parcours normand où l’exposition au vent et les changements de direction peuvent modifier le coût d’une allure pourtant régulière.", "Une course entre ville, littoral et ambiance de la Côte Fleurie.", "https://www.marathondeauville.fr/"),
    _marathon("Marathon du Lac d’Annecy", "Annecy", "France", "france", "Un parcours au bord du lac qui paraît roulant mais peut comporter des faux plats ; vent, température et retour de course doivent être anticipés.", "Un marathon français reconnu pour son cadre entre lac et montagnes.", "https://www.marathon-annecy.com/"),
]


MARATHONS = {item["slug"]: item for item in WORLD_MARATHONS + FRANCE_MARATHONS}
EUROPE_MARATHONS = [item for item in WORLD_MARATHONS if item["region"] == "europe"]
EUROPEAN_MAJORS = [item for item in EUROPE_MARATHONS if item["major"]]

