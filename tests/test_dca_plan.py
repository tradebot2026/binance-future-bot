"""3-step DCA: entry_stage once-only, final SL from Entry 3 trigger."""

from __future__ import annotations

import unittest

from core.dca_plan import (
    apply_initial_dca_plan,
    base_entry_price,
    build_dca_plan,
    dca_sl_wallet_cap_percent,
    entry_stage_of,
    interpolate_toward_sl,
    ladder_stop_loss_usd,
    last_resort_sl_hit,
    mark_dca_filled,
    next_dca_step,
    pick_structural_entries,
    resolved_final_sl,
    should_trigger_add,
    sl_is_armed,
    telegram_dca_levels,
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

    def test_final_sl_uses_base_plan_not_average(self) -> None:
        plan = build_dca_plan(8000.0, 7700.0)
        self.assertEqual(plan["dca_final_sl"], 7700.0)
        self.assertEqual(resolved_final_sl(plan, "LONG"), 7700.0)
        # Averaged 7950 − 300 would be 7650 — must not be used.
        self.assertNotAlmostEqual(plan["dca_final_sl"], 7950.0 - 300.0)

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

    def test_short_final_sl_from_base(self) -> None:
        meta = apply_initial_dca_plan({}, 100.0, 110.0)
        self.assertAlmostEqual(resolved_final_sl(meta, "SHORT"), 110.0)
        self.assertTrue(should_trigger_add("SHORT", meta["dca_entry2_px"], meta, 2))
        self.assertTrue(last_resort_sl_hit("SHORT", 110.0, 110.0))

    def test_does_not_overwrite_existing_base(self) -> None:
        meta = apply_initial_dca_plan({}, 20.0, 18.0)
        apply_initial_dca_plan(meta, 19.0, 17.0)
        self.assertEqual(meta["base_entry_price"], 20.0)
        self.assertEqual(meta["entry_stage"], 1)

    def test_telegram_levels_are_precalculated_from_base(self) -> None:
        meta = apply_initial_dca_plan({}, 8000.0, 7700.0)
        levels = telegram_dca_levels(meta)
        self.assertEqual(levels["base_entry"], 8000.0)
        self.assertAlmostEqual(levels["entry2"], interpolate_toward_sl(8000.0, 7700.0, 0.45))
        self.assertAlmostEqual(levels["entry3"], interpolate_toward_sl(8000.0, 7700.0, 0.75))
        self.assertEqual(levels["final_sl"], 7700.0)

    def test_structural_pullbacks_beat_blind_interpolation(self) -> None:
        entry2, entry3, used = pick_structural_entries(
            8000.0,
            7700.0,
            {"ob_zone_low": 7900.0, "sweep_level": 7780.0},
        )
        self.assertTrue(used)
        self.assertAlmostEqual(entry2, 7900.0)
        self.assertAlmostEqual(entry3, 7780.0)
        plan = build_dca_plan(
            8000.0, 7700.0, structure={"ob_zone_low": 7900.0, "sweep_level": 7780.0}
        )
        self.assertTrue(plan["dca_structural_entries"])
        self.assertAlmostEqual(plan["dca_entry2_px"], 7900.0)
        self.assertAlmostEqual(plan["dca_entry3_px"], 7780.0)
        self.assertEqual(plan["dca_final_sl"], 7700.0)

    def test_wallet_cap_tightens_sl_and_ignores_binance_avg(self) -> None:
        plan = build_dca_plan(
            8000.0,
            7700.0,
            wallet=10_000.0,
            entry1_qty=10.0,
            leverage=20.0,
            side="LONG",
        )
        self.assertTrue(plan["dca_sl_capped"])
        self.assertEqual(plan["dca_uncapped_sl"], 7700.0)
        self.assertGreater(plan["dca_final_sl"], 7700.0)
        self.assertLess(plan["dca_final_sl"], 8000.0)
        cap = 10_000.0 * (dca_sl_wallet_cap_percent() / 100.0)
        loss = ladder_stop_loss_usd(
            8000.0,
            plan["dca_final_sl"],
            plan["dca_entry2_px"],
            plan["dca_entry3_px"],
            10.0,
            10_000.0,
            is_long=True,
        )
        self.assertLessEqual(loss, cap + 1.0)
        self.assertEqual(base_entry_price(plan, fallback=7950.0), 8000.0)
        self.assertNotAlmostEqual(plan["dca_final_sl"], 7950.0 - 300.0)


if __name__ == "__main__":
    unittest.main()
