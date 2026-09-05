import unittest

from app.marathon_seo import (
    EUROPEAN_MAJORS,
    FRANCE_MARATHONS,
    MARATHONS,
    WORLD_MARATHONS,
)


class MarathonSeoCatalogTests(unittest.TestCase):
    def test_catalog_contains_expected_world_and_france_selections(self):
        self.assertEqual(len(WORLD_MARATHONS), 20)
        self.assertEqual(len(FRANCE_MARATHONS), 10)
        self.assertEqual(len(MARATHONS), 30)

    def test_slugs_and_official_links_are_unique(self):
        slugs = [item["slug"] for item in WORLD_MARATHONS + FRANCE_MARATHONS]
        self.assertEqual(len(slugs), len(set(slugs)))
        self.assertTrue(all(item["official_url"].startswith("https://") for item in MARATHONS.values()))

    def test_european_majors_are_london_and_berlin(self):
        self.assertEqual(
            {item["name"] for item in EUROPEAN_MAJORS},
            {"Marathon de Londres", "Marathon de Berlin"},
        )


if __name__ == "__main__":
    unittest.main()
