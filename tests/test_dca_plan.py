"""3-step DCA: entry_stage once-only, final SL from Entry 3 trigger."""

from __future__ import annotations

import unittest

from core.dca_plan import (
    apply_initial_dca_plan,
    base_entry_price,
    build_dca_plan,
    entry_stage_of,
    final_sl_from_entry3,
    interpolate_toward_sl,
    last_resort_sl_hit,
    mark_dca_filled,
    next_dca_step,
    resolved_final_sl,
    should_trigger_add,
    sl_is_armed,
    structural_sl_distance,
)


class TestDcaPlan(unittest.TestCase):
    def test_triggers_ignore_binance_average(self) -> None:
        plan = build_dca_plan(8000.0, 7700.0)
        self.assertEqual(plan["entry_stage"], 1)
        self.assertEqual(plan["base_entry_price"], 8000.0)
        self.assertAlmostEqual(plan["dca_entry2_px"], interpolate_toward_sl(8000.0, 7700.0, 0.45))
        self.assertAlmostEqual(plan["dca_entry3_px"], interpolate_toward_sl(8000.0, 7700.0, 0.75))
        averaged = 7950.0
        self.assertEqual(base_entry_price(plan, fallback=averaged), 8000.0)

    def test_final_sl_uses_entry3_trigger_not_average(self) -> None:
        plan = build_dca_plan(8000.0, 7700.0)
        entry3 = plan["dca_entry3_px"]
        sl_dist = structural_sl_distance(8000.0, 7700.0)
        expected = final_sl_from_entry3(entry3, sl_dist, is_long=True)
        self.assertAlmostEqual(plan["dca_final_sl"], expected)
        self.assertAlmostEqual(expected, entry3 - sl_dist)
        # Averaged 7950 − 300 would be 7650 — must not be used.
        self.assertNotAlmostEqual(expected, 7950.0 - sl_dist)
        self.assertEqual(resolved_final_sl(plan, "LONG"), expected)

    def test_entry_stage_advances_once_then_stops(self) -> None:
        meta = apply_initial_dca_plan({}, 50.0, 40.0)
        self.assertEqual(entry_stage_of(None, meta), 1)
        self.assertEqual(next_dca_step(meta), 2)
        self.assertTrue(should_trigger_add("LONG", meta["dca_entry2_px"], meta, 2))
        self.assertFalse(should_trigger_add("LONG", 49.9, meta, 2))
        mark_dca_filled(meta, 2)
        self.assertEqual(meta["entry_stage"], 2)
        self.assertEqual(next_dca_step(meta), 3)
        self.assertFalse(sl_is_armed(meta))
        mark_dca_filled(meta, 3)
        self.assertEqual(meta["entry_stage"], 3)
        self.assertIsNone(next_dca_step(meta))
        self.assertTrue(sl_is_armed(meta))

    def test_column_and_metadata_agree_after_restart(self) -> None:
        meta = apply_initial_dca_plan({}, 100.0, 90.0)
        mark_dca_filled(meta, 2)
        trade = {"entry_stage": 2, "metadata": meta}
        self.assertEqual(entry_stage_of(trade, meta), 2)
        self.assertEqual(next_dca_step(meta, trade), 3)
        trade["entry_stage"] = 3
        mark_dca_filled(meta, 3)
        self.assertIsNone(next_dca_step(meta, trade))

    def test_short_final_sl_from_entry3(self) -> None:
        meta = apply_initial_dca_plan({}, 100.0, 110.0)
        entry3 = meta["dca_entry3_px"]
        expected = entry3 + 10.0
        self.assertAlmostEqual(resolved_final_sl(meta, "SHORT"), expected)
        self.assertTrue(should_trigger_add("SHORT", meta["dca_entry2_px"], meta, 2))
        self.assertTrue(last_resort_sl_hit("SHORT", expected, expected))

    def test_does_not_overwrite_existing_base(self) -> None:
        meta = apply_initial_dca_plan({}, 20.0, 18.0)
        apply_initial_dca_plan(meta, 19.0, 17.0)
        self.assertEqual(meta["base_entry_price"], 20.0)
        self.assertEqual(meta["entry_stage"], 1)


if __name__ == "__main__":
    unittest.main()
