"""LaunchAgent installation for after-close research and the signed bridge."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

from .config import LabConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAUNCH_AGENT_ROOT = Path.home() / "Library" / "LaunchAgents"
DAILY_LABEL = "com.ensomniac.strategy-lab-daily"
BRIDGE_LABEL = "com.ensomniac.strategy-lab-bridge"


class SchedulerError(RuntimeError):
    """Raised when the local Strategy Lab scheduler cannot be managed safely."""


def _path(label: str) -> Path:
    return LAUNCH_AGENT_ROOT / f"{label}.plist"


def _write_plist(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".plist.tmp")
    with temporary.open("wb") as output:
        plistlib.dump(payload, output, sort_keys=True)
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def install(config: LabConfig) -> dict[str, Any]:
    config.ensure_state_directories()
    config.ensure_bridge_secret()
    logs = config.state_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    python = str(Path(sys.executable).resolve())
    cli = str(PROJECT_ROOT / "strategy_lab.py")
    hour = int(config.section("daily_run")["scheduled_hour_et"])
    minute = int(config.section("daily_run")["scheduled_minute_et"])
    daily = {
        "Label": DAILY_LABEL,
        "ProgramArguments": [python, cli, "run", "scheduled"],
        "WorkingDirectory": str(PROJECT_ROOT),
        "StartCalendarInterval": [
            {"Weekday": weekday, "Hour": hour, "Minute": minute}
            for weekday in range(1, 6)
        ],
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "PYTHONUNBUFFERED": "1",
        },
        "StandardOutPath": str(logs / "daily.log"),
        "StandardErrorPath": str(logs / "daily-error.log"),
        "ProcessType": "Background",
    }
    bridge = {
        "Label": BRIDGE_LABEL,
        "ProgramArguments": [python, cli, "bridge", "once"],
        "WorkingDirectory": str(PROJECT_ROOT),
        "RunAtLoad": True,
        "StartInterval": int(config.section("bridge")["idle_poll_seconds"]),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "PYTHONUNBUFFERED": "1",
        },
        "StandardOutPath": str(logs / "bridge.log"),
        "StandardErrorPath": str(logs / "bridge-error.log"),
        "ProcessType": "Background",
    }
    paths = [(_path(DAILY_LABEL), daily), (_path(BRIDGE_LABEL), bridge)]
    for path, payload in paths:
        _write_plist(path, payload)
        subprocess.run(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(path)],
            check=False,
            capture_output=True,
        )
        loaded = subprocess.run(
            ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if loaded.returncode != 0:
            raise SchedulerError(
                f"launchctl could not load {path.name}: {loaded.stderr.strip()}"
            )
    return {
        "status": "INSTALLED",
        "daily": str(paths[0][0]),
        "bridge": str(paths[1][0]),
    }


def uninstall() -> dict[str, Any]:
    removed: list[str] = []
    for label in (DAILY_LABEL, BRIDGE_LABEL):
        path = _path(label)
        if path.exists():
            subprocess.run(
                ["launchctl", "bootout", f"gui/{os.getuid()}", str(path)],
                check=False,
                capture_output=True,
            )
            path.unlink()
            removed.append(str(path))
    return {"status": "UNINSTALLED", "removed": removed}


def status() -> dict[str, Any]:
    jobs: dict[str, Any] = {}
    for label in (DAILY_LABEL, BRIDGE_LABEL):
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            check=False,
            capture_output=True,
            text=True,
        )
        jobs[label] = {
            "loaded": result.returncode == 0,
            "plist": str(_path(label)),
            "plist_exists": _path(label).exists(),
        }
    return {"status": "READY", "jobs": jobs}
