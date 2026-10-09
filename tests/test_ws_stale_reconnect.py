"""WebSocket ticker-age detection and auto-reconnect on stale streams."""

from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from market_data_hub import MarketDataHub


def _hub_with_running_ws() -> MarketDataHub:
    hub = MarketDataHub(MagicMock())
    hub._ws_running = True
    hub._ws_started_at = time.monotonic() - 120.0
    hub._tickers = {
        f"SYM{i}USDT": {"symbol": f"SYM{i}USDT", "lastPrice": 1.0}
        for i in range(12)
    }
    return hub


class TestWsStaleReconnect(unittest.TestCase):
    def test_stale_threshold_is_60_seconds_on_testnet(self) -> None:
        with patch.object(Config, "WS_STALE_SECONDS", 30), patch.object(
            Config, "USE_TESTNET", True
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            self.assertEqual(MarketDataHub._effective_ticker_stale_seconds(), 60.0)

    def test_stale_threshold_is_30_seconds_on_mainnet(self) -> None:
        with patch.object(Config, "WS_STALE_SECONDS", 30), patch.object(
            Config, "USE_TESTNET", False
        ):
            self.assertEqual(MarketDataHub._effective_ticker_stale_seconds(), 30.0)

    def test_testnet_cached_tickers_reconnect_when_age_exceeds_60s(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", False
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            self.assertTrue(hub.ws_is_stale())
            self.assertTrue(hub._should_reconnect_for_stale_ticker())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "STALE")

    def test_testnet_does_not_reconnect_at_45s_age(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 45.0
        hub._last_book_event_at = time.monotonic()
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", False
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            self.assertFalse(hub.ws_is_stale())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_health_is_healthy_after_fresh_ticker_event(self) -> None:
        hub = _hub_with_running_ws()
        now = time.monotonic()
        hub._last_ticker_event_at = now
        hub._last_book_event_at = now
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", True), patch.object(
            Config, "WS_STALE_SECONDS", 30
        ):
            self.assertFalse(hub.ws_is_stale())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_warming_does_not_reconnect_before_first_tick(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = 0.0
        hub._ws_started_at = time.monotonic()
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False), patch.object(
            Config, "WS_STALE_SECONDS", 30
        ):
            self.assertTrue(hub.is_ws_warming_up())
            self.assertFalse(hub.ws_is_stale())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "WARMING")

    def test_request_reconnect_not_skipped_on_testnet_with_cache(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 120.0
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "WS_RECONNECT_ENABLED", True
        ), patch.object(Config, "ENABLE_WEBSOCKET_STREAMS", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", False
        ), patch(
            "market_data_hub.threading.Thread"
        ) as thread_cls:
            hub._request_reconnect(
                "ticker stream stale — age 120s (threshold 30s) "
                "resetting miniTicker/bookTicker/userData"
            )
            thread_cls.assert_called_once()
            self.assertTrue(hub._reconnect_in_progress)

    def test_reconnect_stamp_clears_stale_without_waiting_for_ticks(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 600.0
        hub._last_book_event_at = time.monotonic() - 600.0
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False):
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "STALE")
            hub._mark_stream_freshness()
            self.assertFalse(hub.ws_is_stale())
            self.assertLess(hub.ticker_cache_age_seconds(), 1.0)
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_heartbeat_frame_keeps_connection_healthy(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        wrapped = hub._wrap_ws_callback(lambda _msg: None, stream="ticker")
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False), patch.object(
            Config, "USE_TESTNET", True
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            wrapped({"ping": True})
            self.assertFalse(hub.ws_is_stale())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_fresh_mini_ticker_message_clears_stale_state(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 600.0
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False):
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "STALE")
            hub._on_ticker_message(
                {
                    "data": [
                        {
                            "s": "BTCUSDT",
                            "c": "50000",
                            "q": "1",
                            "v": "1",
                            "h": "1",
                            "l": "1",
                            "o": "1",
                        }
                    ]
                }
            )
            self.assertGreater(hub._last_ticker_event_at, 0.0)
            self.assertLess(hub.ticker_cache_age_seconds(), 1.0)
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_silent_rest_refresh_does_not_stamp_ws_freshness(self) -> None:
        hub = _hub_with_running_ws()
        now = time.monotonic()
        hub._last_ticker_event_at = now
        hub._last_book_event_at = now
        hub.set_ticker_rest_fetcher(
            lambda: {"BTCUSDT": {"lastPrice": "50000", "quoteVolume": "1"}}
        )
        with patch.object(Config, "ENABLE_REST_TICKER_FALLBACK", True), patch.object(
            Config, "SCAN_WS_ONLY", False
        ), patch.object(Config, "ENABLE_WEBSOCKET_STREAMS", False):
            before = hub._last_ticker_event_at
            count = hub.refresh_ticker_cache_from_rest(silent=True)
        self.assertGreaterEqual(count, 1)
        self.assertEqual(hub._last_ticker_event_at, before)
        self.assertIn("BTCUSDT", hub.get_ticker_map())

    def test_book_ticker_message_updates_book_freshness(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic()
        hub._last_book_event_at = time.monotonic() - 250.0
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", True), patch.object(
            Config, "WS_STALE_SECONDS", 30
        ):
            self.assertTrue(hub._book_stream_is_stale())
            hub._on_book_ticker_message(
                {"data": [{"s": "ETHUSDT", "b": "1.0", "a": "1.1"}]}
            )
            self.assertFalse(hub._book_stream_is_stale())
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "HEALTHY")

    def test_execution_requires_rest_when_ticker_row_is_stale(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic()
        hub._tickers["BTCUSDT"] = {
            "symbol": "BTCUSDT",
            "lastPrice": 50000.0,
            "updated_at": time.monotonic() - 600.0,
        }
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False), patch.object(
            Config, "USE_TESTNET", True
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            self.assertTrue(hub.execution_requires_rest_price("BTCUSDT"))
            self.assertIsNone(hub.get_execution_ticker_price("BTCUSDT"))

    def test_healthy_klines_skip_full_ticker_reconnect(self) -> None:
        from market_data_hub import _KlineMultiplexSocket

        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        hub._last_book_event_at = time.monotonic() - 90.0
        hub._kline_sockets = [
            _KlineMultiplexSocket(
                streams=["ongusdt@kline_5m"],
                last_event_at=time.monotonic(),
            )
        ]
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", True
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60), patch.object(
            hub, "refresh_ticker_cache_from_rest", return_value=12
        ) as rest:
            self.assertTrue(hub._kline_feeds_healthy())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())
            self.assertTrue(hub._should_refresh_ticker_sockets())
            self.assertTrue(hub.ws_state_is_degraded())
            rest.assert_not_called()
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "DEGRADED")

    def test_degraded_ws_skips_ticker_rest_fallback(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        hub._ticker_rest_fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": 1}})
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "WS_STALE_SECONDS_TESTNET", 60
        ):
            hub.refresh_ticker_cache_from_rest()
        hub._ticker_rest_fetcher.assert_not_called()

    def test_stale_reconnect_cooldown_skips_repeat(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        hub._last_stale_reconnect_success_at = time.monotonic()
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", False
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60), patch.object(
            Config, "WS_STALE_RECONNECT_COOLDOWN_SECONDS", 180.0
        ), patch.object(hub, "refresh_ticker_cache_from_rest", return_value=12):
            self.assertTrue(hub._stale_reconnect_on_cooldown())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())

    def test_reconnect_and_warmup_skip_ticker_rest(self) -> None:
        hub = _hub_with_running_ws()
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        hub._reconnect_in_progress = True
        self.assertEqual(hub.refresh_ticker_cache_from_rest(silent=True), 12)
        fetcher.assert_not_called()
        hub._reconnect_in_progress = False
        hub._last_ticker_event_at = 0.0
        hub._ws_started_at = time.monotonic()
        self.assertTrue(hub.is_ws_warming_up())
        self.assertEqual(hub.refresh_ticker_cache_from_rest(silent=True), 12)
        fetcher.assert_not_called()

    def test_quiet_user_stream_does_not_reconnect_when_flat(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_user_event_at = 0.0
        hub._last_user_socket_at = 0.0
        hub._ws_started_at = time.monotonic() - 600.0
        hub._positions = []
        with patch.object(Config, "USE_TESTNET", True):
            self.assertFalse(hub.user_stream_has_account_data())
            self.assertFalse(hub.user_stream_is_stale())
            self.assertFalse(hub._should_reconnect_for_stale_user_stream())

    def test_listen_key_keepalive_runs_even_if_governor_blocks(self) -> None:
        hub = _hub_with_running_ws()
        hub._listen_key = "test-listen-key"
        hub._last_listen_keepalive_at = time.monotonic() - 1900.0
        hub._rest_governor = SimpleNamespace(
            can_make_background_rest_call=lambda _weight: False
        )
        keepalive = MagicMock()
        hub.client.futures_stream_keepalive = keepalive
        hub._maybe_keepalive_user_listen_key()
        keepalive.assert_called_once_with(listenKey="test-listen-key")

    def test_listen_key_keepalive_skips_during_rest_ban(self) -> None:
        hub = _hub_with_running_ws()
        hub._listen_key = "test-listen-key"
        hub._last_listen_keepalive_at = time.monotonic() - 1900.0
        hub._rest_blocked_until = time.time() + 60.0
        keepalive = MagicMock()
        hub.client.futures_stream_keepalive = keepalive
        hub._maybe_keepalive_user_listen_key()
        keepalive.assert_not_called()

    def test_full_ws_kline_buffer_skips_rest(self) -> None:
        hub = _hub_with_running_ws()
        rows = []
        for i in range(260):
            rows.append(
                {
                    "timestamp": pd.Timestamp("2026-01-01") + pd.Timedelta(minutes=5 * i),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.2,
                    "volume": 1_000.0,
                }
            )
        hub.seed_klines_from_dataframe("ETHUSDT", "5m", pd.DataFrame(rows))
        rest_fetcher = MagicMock(return_value=pd.DataFrame(rows[:50]))
        with patch.object(Config, "WS_KLINE_BOOTSTRAP_MIN_BARS", 250):
            df = hub.get_candles("ETHUSDT", "5m", 280, rest_fetcher, allow_rest=True)
        self.assertGreaterEqual(len(df), 250)
        rest_fetcher.assert_not_called()

    def test_scan_depth_does_not_skip_backtest_rest(self) -> None:
        hub = _hub_with_running_ws()
        rows = []
        for i in range(280):
            rows.append(
                {
                    "timestamp": pd.Timestamp("2026-01-01")
                    + pd.Timedelta(minutes=15 * i),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.2,
                    "volume": 1_000.0,
                }
            )
        hub.seed_klines_from_dataframe("ETHUSDT", "15m", pd.DataFrame(rows))
        filled = rows + [
            {
                "timestamp": pd.Timestamp("2026-01-01")
                + pd.Timedelta(minutes=15 * (280 + i)),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.2,
                "volume": 1_000.0,
            }
            for i in range(220)
        ]
        rest_fetcher = MagicMock(return_value=pd.DataFrame(filled))
        with patch.object(Config, "SCAN_WS_ONLY", False), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", False
        ):
            df = hub.get_candles("ETHUSDT", "15m", 500, rest_fetcher, allow_rest=True)
        rest_fetcher.assert_called_once()
        self.assertGreaterEqual(len(df), 450)

    def test_cache_miss_does_not_rest_during_reconnect(self) -> None:
        hub = _hub_with_running_ws()
        hub._reconnect_in_progress = True
        rest_fetcher = MagicMock(return_value=None)
        df = hub.get_candles("ETHUSDT", "5m", 50, rest_fetcher, allow_rest=True)
        self.assertTrue(df.empty)
        rest_fetcher.assert_not_called()

    def test_stopped_ws_is_degraded_and_skips_ticker_rest(self) -> None:
        hub = _hub_with_running_ws()
        hub._ws_running = False
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        self.assertEqual(hub.get_ws_health_snapshot()["state"], "STOPPED")
        self.assertTrue(hub.ws_is_degraded())
        self.assertTrue(hub.ws_state_is_degraded())
        self.assertFalse(hub.ws_blocks_priority_scan())
        self.assertTrue(hub._should_recover_stopped_ws())
        self.assertEqual(hub.refresh_ticker_cache_from_rest(silent=True), 12)
        fetcher.assert_not_called()

    def test_stopped_ws_schedules_reconnect(self) -> None:
        hub = _hub_with_running_ws()
        hub._ws_running = False
        hub._last_reconnect_request_at = 0.0
        with patch.object(Config, "WS_RECONNECT_ENABLED", True), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", True
        ), patch.object(Config, "WS_RECONNECT_DEBOUNCE_SECONDS", 0.0), patch(
            "market_data_hub.threading.Thread"
        ) as thread_cls:
            hub._request_reconnect("ws STOPPED — auto-reconnect")
        thread_cls.assert_called_once()
        self.assertTrue(hub._reconnect_in_progress)

    def test_cache_miss_does_not_rest_when_ws_stopped(self) -> None:
        hub = _hub_with_running_ws()
        hub._ws_running = False
        rest_fetcher = MagicMock(return_value=None)
        df = hub.get_candles("ETHUSDT", "5m", 50, rest_fetcher, allow_rest=True)
        self.assertTrue(df.empty)
        rest_fetcher.assert_not_called()

    def test_persistent_degraded_escalates_to_full_reconnect(self) -> None:
        from market_data_hub import _KlineMultiplexSocket

        hub = _hub_with_running_ws()
        now = time.monotonic()
        hub._last_ticker_event_at = now - 90.0
        hub._last_book_event_at = now - 90.0
        hub._last_ws_seen_at = now - 90.0
        hub._kline_sockets = [
            _KlineMultiplexSocket(
                streams=["ethusdt@kline_5m"],
                last_event_at=now,
            )
        ]
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "ENABLE_WS_BOOK_STREAM", True
        ), patch.object(Config, "WS_STALE_SECONDS_TESTNET", 60):
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "DEGRADED")
            self.assertTrue(hub._should_refresh_ticker_sockets())
            self.assertFalse(hub._should_escalate_degraded_reconnect())
            hub._last_ticker_socket_refresh_at = now
            self.assertFalse(hub._should_refresh_ticker_sockets())
            self.assertTrue(hub._should_escalate_degraded_reconnect())

    def test_scan_keeps_running_on_ws_cache_during_reconnect(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        hub = _hub_with_running_ws()
        hub._ws_running = False
        orch._hub = hub
        halted, reason = orch._scan_gate_open()
        self.assertFalse(halted)
        self.assertEqual(reason, "")
        hub._ws_running = True
        hub._reconnect_in_progress = True
        halted, reason = orch._scan_gate_open()
        self.assertFalse(halted)

    def test_ws_ping_interval_is_15_to_20_seconds(self) -> None:
        with patch.object(Config, "WS_PING_INTERVAL_SECONDS", 5.0):
            self.assertEqual(Config.ws_ping_interval_seconds(), 15.0)
        with patch.object(Config, "WS_PING_INTERVAL_SECONDS", 30.0):
            self.assertEqual(Config.ws_ping_interval_seconds(), 20.0)
        with patch.object(Config, "WS_PING_INTERVAL_SECONDS", 20.0):
            self.assertEqual(Config.ws_ping_interval_seconds(), 20.0)
        with patch.object(Config, "WS_PING_TIMEOUT_SECONDS", 30.0):
            self.assertEqual(Config.ws_ping_timeout_seconds(), 10.0)
            self.assertLess(Config.ws_ping_timeout_seconds(), Config.ws_ping_interval_seconds())

    def test_protocol_ping_stamps_ticker_book_so_180s_stale_does_not_fire(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 200.0
        hub._last_book_event_at = time.monotonic() - 200.0
        hub._last_ws_seen_at = time.monotonic() - 200.0
        hub._last_protocol_ping_at = 0.0
        loop = MagicMock()
        loop.is_running.return_value = True
        hub._ws_loop = loop
        ping = MagicMock()
        sock = SimpleNamespace(ws=SimpleNamespace(ping=ping))
        hub._ws_manager = SimpleNamespace(
            _bsm=SimpleNamespace(_conns={"miniTicker": sock})
        )
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "WS_STALE_SECONDS_TESTNET", 180
        ), patch.object(Config, "ENABLE_WS_BOOK_STREAM", True), patch(
            "market_data_hub.asyncio.run_coroutine_threadsafe"
        ) as scheduled:
            self.assertTrue(hub.ws_is_stale())
            hub._maybe_protocol_ping()
            self.assertTrue(scheduled.called)
            self.assertFalse(hub.ws_is_stale())
            self.assertFalse(hub._book_stream_is_stale())
            self.assertFalse(hub._should_reconnect_for_stale_ticker())
            self.assertLess(hub.ticker_cache_age_seconds(), 2.0)

    def test_expired_ban_timestamp_clears_halt_and_hard_resubscribes(self) -> None:
        hub = _hub_with_running_ws()
        expired_ms = int((time.time() - 5.0) * 1000)
        hub.block_rest_for_ban("banned until %s" % expired_ms, banned_until_ms=expired_ms)
        self.assertTrue(hub._rest_ban_active)
        self.assertGreater(hub._rest_blocked_until, time.time())
        with patch.object(hub, "force_hard_resubscribe") as hard:
            blocked, _ = hub.is_rest_blocked()
            halted, _ = hub.is_scan_halted()
        self.assertFalse(blocked)
        self.assertFalse(halted)
        self.assertFalse(hub._rest_ban_active)
        self.assertIsNone(hub.get_ban_status())
        hard.assert_called()

    def test_force_hard_resubscribe_starts_worker(self) -> None:
        hub = _hub_with_running_ws()
        with patch.object(Config, "WS_RECONNECT_ENABLED", True), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", True
        ), patch("market_data_hub.threading.Thread") as thread_cls:
            hub.force_hard_resubscribe("REST ban expired — hard WS resubscribe")
        thread_cls.assert_called_once()
        self.assertTrue(hub._reconnect_in_progress)
        self.assertEqual(
            thread_cls.call_args.kwargs["args"],
            ("REST ban expired — hard WS resubscribe", False, True),
        )

    def test_emergency_rest_bridge_runs_when_ws_stale(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 200.0
        hub._last_emergency_rest_at = 0.0
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "WS_STALE_SECONDS_TESTNET", 60
        ), patch.object(Config, "ENABLE_WS_BOOK_STREAM", False):
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "STALE")
            count = hub.maybe_emergency_rest_bridge()
        self.assertGreater(count, 0)
        fetcher.assert_called_once()

    def test_emergency_rest_bridge_skips_when_healthy(self) -> None:
        hub = _hub_with_running_ws()
        now = time.monotonic()
        hub._last_ticker_event_at = now
        hub._last_book_event_at = now
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        self.assertEqual(hub.maybe_emergency_rest_bridge(), 0)
        fetcher.assert_not_called()

    def test_listener_dispatch_is_off_ws_thread(self) -> None:
        hub = _hub_with_running_ws()
        seen: list[tuple[str, float]] = []
        hub.register_price_tick_listener(lambda s, p: seen.append((s, p)))
        hub._emit_price_tick("BTCUSDT", 1.25)
        deadline = time.monotonic() + 2.0
        while not seen and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(seen, [("BTCUSDT", 1.25)])

    def test_kline_bootstrap_log_tag_is_not_warmup(self) -> None:
        import inspect

        from pipeline import event_scan_orchestrator as orch_mod

        source = inspect.getsource(orch_mod)
        self.assertNotIn("SCAN_KLINE_WARMUP", source)
        self.assertIn("SCAN_KLINE_BOOTSTRAP", source)

    def test_scan_ws_only_forbids_healthy_ticker_and_kline_rest(self) -> None:
        hub = _hub_with_running_ws()
        now = time.monotonic()
        hub._last_ticker_event_at = now
        hub._last_book_event_at = now
        hub._last_ws_seen_at = now
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        rest_klines = MagicMock(return_value=None)
        with patch.object(Config, "SCAN_WS_ONLY", True), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", True
        ), patch.object(Config, "ENABLE_REST_TICKER_FALLBACK", True):
            self.assertFalse(hub.ws_state_is_degraded())
            self.assertTrue(hub.ws_forbids_market_rest())
            self.assertFalse(hub.needs_ticker_rest_fallback())
            self.assertEqual(hub.refresh_ticker_cache_from_rest(silent=True), 12)
            fetcher.assert_not_called()
            df = hub.get_candles("ETHUSDT", "5m", 50, rest_klines, allow_rest=True)
        self.assertTrue(df.empty)
        rest_klines.assert_not_called()

    def test_stale_ws_forbids_market_rest_even_if_scan_ws_only_off(self) -> None:
        hub = _hub_with_running_ws()
        hub._last_ticker_event_at = time.monotonic() - 90.0
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "WS_STALE_SECONDS_TESTNET", 60
        ), patch.object(Config, "SCAN_WS_ONLY", False), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", True
        ), patch.object(Config, "ENABLE_REST_TICKER_FALLBACK", True):
            self.assertTrue(hub.ws_state_is_degraded())
            self.assertTrue(hub.ws_forbids_market_rest())
            hub.refresh_ticker_cache_from_rest(force=True)
        fetcher.assert_not_called()

    def test_wait_until_ready_skips_rest_when_ws_streams_enabled(self) -> None:
        hub = _hub_with_running_ws()
        hub._tickers = {}
        fetcher = MagicMock(return_value={"BTCUSDT": {"lastPrice": "1"}})
        hub.set_ticker_rest_fetcher(fetcher)
        with patch.object(Config, "ENABLE_WEBSOCKET_STREAMS", True), patch.object(
            Config, "WS_STARTUP_WAIT_SECONDS", 0
        ), patch.object(Config, "TICKER_REST_FALLBACK_AFTER_SECONDS", 1):
            ready = hub.wait_until_ready(timeout_seconds=0, min_symbols=1)
        self.assertFalse(ready)
        fetcher.assert_not_called()

    def test_tier1_refresh_defaults_to_ws_cache_only(self) -> None:
        from inspect import signature

        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        default = signature(EventScanOrchestrator.refresh_tier1_universe).parameters[
            "allow_rest"
        ].default
        self.assertFalse(default)

    def test_raw_list_miniticker_payload_fills_cache(self) -> None:
        hub = MarketDataHub(MagicMock())
        hub._ws_running = True
        wrapped = hub._wrap_ws_callback(hub._on_ticker_message, stream="ticker")
        wrapped(
            [
                {
                    "s": "BTCUSDT",
                    "c": "50000",
                    "q": "10",
                    "v": "1",
                    "h": "1",
                    "l": "1",
                    "o": "1",
                }
            ]
        )
        self.assertIn("BTCUSDT", hub.get_ticker_map())
        self.assertEqual(hub.get_ticker_map()["BTCUSDT"]["lastPrice"], 50000.0)

    def test_book_ticker_list_seeds_ticker_cache(self) -> None:
        hub = MarketDataHub(MagicMock())
        hub._ws_running = True
        wrapped = hub._wrap_ws_callback(hub._on_book_ticker_message, stream="book")
        wrapped([{"s": "ETHUSDT", "b": "1.0", "a": "1.2"}])
        self.assertIn("ETHUSDT", hub.get_ticker_map())
        self.assertAlmostEqual(hub.get_ticker_map()["ETHUSDT"]["lastPrice"], 1.1)
        self.assertTrue(hub.has_ws_book_data())

    def test_wait_quietly_for_empty_ticker_cache_is_fast(self) -> None:
        hub = MarketDataHub(MagicMock())
        started = time.monotonic()
        ready = hub.wait_quietly_for_ticker_cache(timeout_seconds=0.0)
        self.assertFalse(ready)
        self.assertLess(time.monotonic() - started, 0.5)

    def test_tier1_empty_cache_logs_at_most_every_45s(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        hub = MarketDataHub(MagicMock())
        orch._hub = hub
        orch._tier1_symbols = []
        orch._last_universe_refresh_at = 0.0
        orch._last_ticker_unavail_log_at = 0.0
        orch.exchange = MagicMock()
        with patch("pipeline.event_scan_orchestrator.scanner_logger") as log:
            orch.refresh_tier1_universe(force=True, allow_rest=False)
            orch.refresh_tier1_universe(force=True, allow_rest=False)
        self.assertEqual(log.info.call_count, 1)
        self.assertIn("Waiting on WS miniTicker/bookTicker", log.info.call_args[0][0])

    def test_universe_rest_seed_runs_once_even_when_ws_only(self) -> None:
        hub = MarketDataHub(MagicMock())
        calls: list[int] = []

        def fetcher() -> dict[str, dict[str, str]]:
            calls.append(1)
            return {
                "BTCUSDT": {"lastPrice": "50000", "quoteVolume": "1000000"},
                "ETHUSDT": {"lastPrice": "3000", "quoteVolume": "800000"},
            }

        with patch.object(Config, "SCAN_WS_ONLY", True), patch.object(
            Config, "STARTUP_TICKER_REST_SEED", True
        ):
            first = hub.seed_universe_from_rest_once(fetcher, min_symbols=2)
            second = hub.seed_universe_from_rest_once(fetcher, min_symbols=2)
        self.assertEqual(len(calls), 1)
        self.assertGreaterEqual(first, 2)
        self.assertEqual(second, first)
        self.assertIn("BTCUSDT", hub.get_ticker_map())
        self.assertGreater(hub.volume_ranked_ticker_count(), 0)
        self.assertEqual(hub._last_ticker_event_at, 0.0)

    def test_universe_rest_seed_skipped_when_volume_already_present(self) -> None:
        hub = _hub_with_running_ws()
        for row in hub._tickers.values():
            row["quoteVolume"] = 1_000_000.0
        fetcher = MagicMock(
            return_value={"BTCUSDT": {"lastPrice": "1", "quoteVolume": "1"}}
        )
        hub.seed_universe_from_rest_once(fetcher, min_symbols=10)
        fetcher.assert_not_called()

    def test_empty_universe_priority_scan_logs_at_most_every_60s(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = None
        orch._tier1_symbols = []
        orch._last_empty_universe_log_at = 0.0
        orch._scan_gate_open = lambda: (False, "")  # type: ignore[method-assign]
        orch.maybe_refresh_tier1_periodic = lambda: None  # type: ignore[method-assign]
        orch.bootstrap_watchlist_once = lambda: []  # type: ignore[method-assign]
        orch.run_catchup = lambda: 0  # type: ignore[method-assign]
        orch.conflict_guard = MagicMock()
        orch._fast_track_live_spikes = lambda: None  # type: ignore[method-assign]
        orch._drain_due_event_symbols = lambda: ({}, [])  # type: ignore[method-assign]
        orch._cycle_scan_symbols = lambda extra: []  # type: ignore[method-assign]
        orch.assignment_manager = MagicMock(tier2_size=0)
        with patch.object(Config, "ENABLE_THREE_TIER_FUNNEL", False), patch(
            "pipeline.event_scan_orchestrator.scanner_logger"
        ) as log:
            orch.process_priority_scan_cycle()
            orch.process_priority_scan_cycle()
        self.assertEqual(log.info.call_count, 1)
        self.assertIn("empty universe", log.info.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
