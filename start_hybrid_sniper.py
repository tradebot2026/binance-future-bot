#!/usr/bin/env python3
"""Start watchdog.py and main.py as sibling processes.

main.py must NOT spawn watchdog.py (a child dies with the parent).
This launcher starts both as independent processes, then exits.

Usage (from the project directory):
  python start_hybrid_sniper.py

Equivalent manual startup (preferred on a VPS):
  screen/tmux window 1: python watchdog.py
  screen/tmux window 2: python main.py

Linux systemd unit templates (not installed by this script):
  deploy/systemd/hybrid-sniper-watchdog.service
  deploy/systemd/hybrid-sniper-main.service

This script does not enable Live trading. Native TP/SL defaults ON.
It does not place orders. Duplicate main.py is still blocked by the instance lock.
"""

from __future__ import annotations

import os
import sys
import time

from core.process_spawn import spawn_detached_python

ROOT = os.path.dirname(os.path.abspath(__file__))


def _popen(script: str):
    return spawn_detached_python(os.path.join(ROOT, script), cwd=ROOT)


def main() -> int:
    print("HYBRID_SNIPER supervisor launcher")
    print("Starting sibling watchdog.py then main.py.")
    print("Native exchange TP/SL defaults ON unless ENABLE_NATIVE_TP_SL=false.")
    watchdog = _popen("watchdog.py")
    time.sleep(1.5)
    if watchdog.poll() is not None:
        print(
            f"ERROR: watchdog.py exited immediately (code={watchdog.returncode}). "
            "Start it in its own terminal: python watchdog.py"
        )
        return 1
    bot = _popen("main.py")
    time.sleep(1.5)
    if bot.poll() is not None:
        print(
            f"ERROR: main.py exited immediately (code={bot.returncode}). "
            "Start it in its own terminal: python main.py"
        )
        return 1
    print(f"watchdog.py pid={watchdog.pid}")
    print(f"main.py pid={bot.pid}")
    print("Launcher exiting — both processes are independent of this shell.")
    print("If main.py reports WATCHDOG_PROCESS_ABSENT, this start failed; use two terminals.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
