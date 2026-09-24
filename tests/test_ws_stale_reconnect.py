"""WebSocket ticker-age detection and auto-reconnect on stale streams."""

from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
        hub._last_ticker_event_at = time.monotonic() - 90.0
        hub.set_ticker_rest_fetcher(
            lambda: {"BTCUSDT": {"lastPrice": "50000", "quoteVolume": "1"}}
        )
        with patch.object(Config, "ENABLE_REST_TICKER_FALLBACK", True):
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
            rest.assert_not_called()
            self.assertEqual(hub.get_ws_health_snapshot()["state"], "DEGRADED")

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

    def test_cache_miss_does_not_rest_during_reconnect(self) -> None:
        hub = _hub_with_running_ws()
        hub._reconnect_in_progress = True
        rest_fetcher = MagicMock(return_value=None)
        df = hub.get_candles("ETHUSDT", "5m", 50, rest_fetcher, allow_rest=True)
        self.assertTrue(df.empty)
        rest_fetcher.assert_not_called()


if __name__ == "__main__":
    unittest.main()
