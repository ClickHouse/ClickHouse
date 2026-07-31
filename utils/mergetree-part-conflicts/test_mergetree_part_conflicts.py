#!/usr/bin/env python3
"""Tests for mergetree_part_conflicts. Run: python3 -m unittest -v (from this dir)."""

import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mergetree_part_conflicts as m  # noqa: E402


def P(name, fv=m.FORMAT_VERSION_CUSTOM):
    info = m.parse_part_name(name, fv)
    assert info is not None, name
    return info


class TestParsing(unittest.TestCase):
    def test_new_format_no_mutation(self):
        p = P("20260722_98_20874_190")
        self.assertEqual((p.partition_id, p.min_block, p.max_block, p.level, p.mutation),
                         ("20260722", 98, 20874, 190, 0))

    def test_new_format_with_mutation(self):
        p = P("20260722_98_20874_190_5")
        self.assertEqual((p.partition_id, p.min_block, p.max_block, p.level, p.mutation),
                         ("20260722", 98, 20874, 190, 5))

    def test_all_partition(self):
        p = P("all_1_1_0")
        self.assertEqual((p.partition_id, p.min_block, p.max_block, p.level), ("all", 1, 1, 0))

    def test_hex_hash_partition(self):
        p = P("a1b2c3d4e5f6_10_20_3")
        self.assertEqual((p.partition_id, p.min_block, p.max_block, p.level), ("a1b2c3d4e5f6", 10, 20, 3))

    def test_old_format(self):
        p = P("20140317_20140323_2_2_0", fv=m.FORMAT_VERSION_OLD)
        self.assertEqual((p.partition_id, p.min_block, p.max_block, p.level), ("201403", 2, 2, 0))

    def test_not_a_part(self):
        for bad in ("detached", "format_version.txt", "tmp_insert_123", "notapart", ""):
            self.assertIsNone(m.parse_part_name(bad))


class TestContains(unittest.TestCase):
    def test_merge_ancestor_contains_source(self):
        # 0_100_1 is a merge over 0_50_0 and 51_100_0
        self.assertTrue(P("all_0_100_1").contains(P("all_0_50_0")))
        self.assertTrue(P("all_0_100_1").contains(P("all_51_100_0")))

    def test_equal_range_needs_higher_or_equal_level(self):
        # equal block range: strictly_contains satisfied by equal range; level>= required
        self.assertTrue(P("all_0_5_3").contains(P("all_0_5_2")))
        self.assertFalse(P("all_0_4_2").contains(P("all_0_5_2")))  # narrower range

    def test_wider_but_equal_level_does_not_contain(self):
        # all_0_5_2 does not contain all_0_4_2 (needs level>rhs.level unless equal range)
        self.assertFalse(P("all_0_5_2").contains(P("all_0_4_2")))

    def test_max_level_contains(self):
        self.assertTrue(P(f"all_0_100_{m.MAX_LEVEL}").contains(P("all_10_20_50")))

    def test_different_partition_never_contains(self):
        self.assertFalse(P("20260722_0_100_5").contains(P("20260723_0_50_0")))

    def test_mutation_must_be_ge(self):
        self.assertFalse(P("all_0_100_5_1").contains(P("all_0_50_0_3")))
        self.assertTrue(P("all_0_100_5_3").contains(P("all_0_50_0_1")))


class TestDisjoint(unittest.TestCase):
    def test_disjoint(self):
        self.assertTrue(P("all_0_50_0").is_disjoint(P("all_51_100_0")))

    def test_touching_ranges_overlap(self):
        self.assertFalse(P("all_0_50_0").is_disjoint(P("all_50_100_0")))

    def test_different_partition_is_disjoint(self):
        self.assertTrue(P("20260722_0_50_0").is_disjoint(P("20260723_0_50_0")))


class TestClassify(unittest.TestCase):
    def test_customer_partial_overlap(self):
        # Turkcell nat, partition 20260722: neither contains the other -> load-aborting conflict.
        parts = [P("20260722_98_20874_190"), P("20260722_2313_113249_107")]
        rep = m.classify_partition(parts)
        self.assertEqual(len(rep.conflicts), 1)
        self.assertEqual(rep.conflicts[0].overlap(), (2313, 20874))
        self.assertEqual(len(rep.maximal), 2)
        self.assertEqual(rep.covered, [])

    def test_healthy_merge_layers_no_conflict(self):
        parts = [P("all_0_100_1"), P("all_0_50_0"), P("all_51_100_0")]
        rep = m.classify_partition(parts)
        self.assertEqual(rep.conflicts, [])
        self.assertEqual([p.name for p in rep.maximal], ["all_0_100_1"])
        self.assertEqual(sorted(p.name for p in rep.covered), ["all_0_50_0", "all_51_100_0"])

    def test_false_dominator_shows_as_covered_not_conflict(self):
        # A wider, higher-level part name-covers another -> silent path, reported as covered.
        parts = [P("20260722_98_113249_200"), P("20260722_2313_20874_107")]
        rep = m.classify_partition(parts)
        self.assertEqual(rep.conflicts, [])
        self.assertEqual([p.name for p in rep.maximal], ["20260722_98_113249_200"])
        self.assertEqual([p.name for p in rep.covered], ["20260722_2313_20874_107"])

    def test_disjoint_parts_no_conflict(self):
        parts = [P("all_0_50_0"), P("all_51_100_0"), P("all_101_150_0")]
        rep = m.classify_partition(parts)
        self.assertEqual(rep.conflicts, [])
        self.assertEqual(len(rep.maximal), 3)

    def test_three_way_overlap_reports_all_pairs(self):
        parts = [P("all_0_50_1"), P("all_40_90_1"), P("all_80_120_1")]
        rep = m.classify_partition(parts)
        # (0_50,40_90) and (40_90,80_120) overlap; (0_50,80_120) do not.
        self.assertEqual(len(rep.conflicts), 2)

    def test_classify_groups_by_partition(self):
        parts = [P("20260722_98_20874_190"), P("20260722_2313_113249_107"),
                 P("20260723_0_10_0")]
        groups = m.classify(parts)
        self.assertTrue(groups["20260722"].has_conflicts)
        self.assertFalse(groups["20260723"].has_conflicts)


class TestSuggestKeep(unittest.TestCase):
    def test_prefers_higher_level(self):
        keep = m.suggest_keep([P("all_0_100_2"), P("all_0_100_5")])
        self.assertEqual(keep.level, 5)

    def test_prefers_wider_when_same_level(self):
        keep = m.suggest_keep([P("all_0_50_3"), P("all_0_100_3")])
        self.assertEqual(keep.max_block, 100)


class TestScan(unittest.TestCase):
    def _make_table(self, root, rel, fv, parts, extras=()):
        tdir = os.path.join(root, rel)
        os.makedirs(tdir)
        with open(os.path.join(tdir, "format_version.txt"), "w") as f:
            f.write(str(fv))
        for name in list(parts) + list(extras):
            os.makedirs(os.path.join(tdir, name))
        return tdir

    def test_scan_skips_non_parts(self):
        with tempfile.TemporaryDirectory() as root:
            tdir = self._make_table(
                root, "store/b11/b11e7407", 1,
                parts=["20260722_98_20874_190", "20260722_2313_113249_107"],
                extras=["detached", "tmp_insert_9", "delete_tmp_5", "broken_x"],
            )
            parts, skipped = m.scan_table_dir(tdir)
            self.assertEqual(sorted(p.name for p in parts),
                             ["20260722_2313_113249_107", "20260722_98_20874_190"])
            self.assertEqual(skipped, [])  # non-parts are filtered by name, not counted as skipped

    def test_find_table_dirs_atomic_layout(self):
        with tempfile.TemporaryDirectory() as root:
            self._make_table(root, "store/b11/b11e7407", 1, parts=["all_1_1_0"])
            self._make_table(root, "store/120/1206e97e", 1, parts=["all_1_1_0"])
            found = m.find_table_dirs(root)
            self.assertEqual(len(found), 2)

    def test_scan_reads_old_format_version(self):
        with tempfile.TemporaryDirectory() as root:
            tdir = self._make_table(root, "data/db/t", 0,
                                    parts=["20140317_20140323_2_2_0"])
            parts, _ = m.scan_table_dir(tdir)
            self.assertEqual(len(parts), 1)
            self.assertEqual(parts[0].partition_id, "201403")


class TestEmitDetachCommands(unittest.TestCase):
    def test_scan_mode_emits_mv(self):
        with tempfile.TemporaryDirectory() as root:
            tdir = os.path.join(root, "store/b11/b11e7407")
            os.makedirs(tdir)
            with open(os.path.join(tdir, "format_version.txt"), "w") as f:
                f.write("1")
            for name in ("20260722_98_20874_190", "20260722_2313_113249_107"):
                os.makedirs(os.path.join(tdir, name))
            parts, _ = m.scan_table_dir(tdir)
            script = "\n".join(m.emit_detach_commands({"t": parts}))
            # keep the higher-level _190, detach the _107, with a concrete mv into detached/
            self.assertIn("mv -- ", script)
            self.assertIn("20260722_2313_113249_107", script)
            self.assertIn(os.path.join(tdir, "detached", "20260722_2313_113249_107"), script)
            self.assertNotIn("mv -- '" + os.path.join(tdir, "20260722_98_20874_190"), script)
            self.assertIn("ATTACH PART '20260722_2313_113249_107'", script)

    def test_stdin_mode_no_path_placeholder(self):
        parts = [P("20260722_98_20874_190"), P("20260722_2313_113249_107")]
        script = "\n".join(m.emit_detach_commands({"t": parts}))
        self.assertIn("path unknown", script)
        self.assertNotIn("mv -- ", script)

    def test_sh_quote_escapes(self):
        self.assertEqual(m._sh_quote("a'b"), "'a'\\''b'")


class TestMainStdin(unittest.TestCase):
    def _run(self, text, *args):
        old_in, old_out = sys.stdin, sys.stdout
        sys.stdin = io.StringIO(text)
        sys.stdout = io.StringIO()
        try:
            code = m.main(["--stdin", *args])
            return code, sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout = old_in, old_out

    def test_conflict_exit_code_and_json(self):
        text = "t\t20260722_98_20874_190\nt\t20260722_2313_113249_107\n"
        code, out = self._run(text, "--json")
        self.assertEqual(code, 2)
        import json
        report = json.loads(out)
        self.assertTrue(report["has_conflicts"])
        part = report["tables"]["t"]["partitions"]["20260722"]
        self.assertEqual(part["conflicts"][0]["overlap_blocks"], [2313, 20874])
        self.assertEqual(part["suggested_detach"], ["20260722_2313_113249_107"])

    def test_clean_exit_code(self):
        text = "t\tall_0_50_0\nt\tall_51_100_0\n"
        code, out = self._run(text)
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
