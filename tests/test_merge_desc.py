import unittest

from app.logic import merge_desc, normalize_summary_block_layout


class MergeDescriptionTests(unittest.TestCase):
    def test_keeps_footer_lines_visually_separated(self):
        block = normalize_summary_block_layout(
            "⛰️ VAM max : 700 m/h\n"
            "⛰️ Montée la plus longue : 1,4 km\n"
            "\nVoir l'analyse complète : https://example.com/activity/1\n"
            "\nPour tous les fans de data —> Join us : https://example.com/"
        )

        self.assertIn(
            "⛰️ Montée la plus longue : 1,4 km\n\n"
            "Voir l'analyse complète : https://example.com/activity/1\n\n"
            "Pour tous les fans de data —> Join us : https://example.com/",
            block,
        )

    def test_keeps_existing_user_description_first(self):
        result = merge_desc("Mon commentaire\n\nBloc d'un autre service", "⛰️ VAM : 700 m/h")

        self.assertEqual(
            result,
            "Mon commentaire\n\nBloc d'un autre service\n\n⛰️ VAM : 700 m/h",
        )

    def test_replaces_own_block_and_preserves_content_after_it(self):
        existing = (
            "Mon commentaire\n\n"
            "⛰️ VAM : 600 m/h\n"
            "Pour tous les fans de data —> Join us : https://www.runningdataplan.com/\n\n"
            "Bloc d'un autre service"
        )

        result = merge_desc(existing, "⛰️ VAM : 700 m/h")

        self.assertEqual(
            result,
            "Mon commentaire\n\nBloc d'un autre service\n\n⛰️ VAM : 700 m/h",
        )


if __name__ == "__main__":
    unittest.main()
