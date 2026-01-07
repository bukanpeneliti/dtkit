import sys
import subprocess
import os
from pathlib import Path


def _get_venv_path() -> Path:
    # Use a location that is definitely accessible
    return Path.home() / ".dtparquet_venv"


def _get_uv_path() -> str:
    # Try common installation paths on Windows
    paths = [
        Path.home() / ".local" / "bin" / "uv.exe",
        Path.home() / "AppData" / "Local" / "bin" / "uv.exe",
        Path("C:/Users/hafiz/.local/bin/uv.exe"),
    ]
    for p in paths:
        if p.exists():
            return str(p)
    return "uv"


def find_stata_path() -> str | None:
    base_dir = Path("C:/Program Files")
    if not base_dir.exists():
        return None
    stata_dirs = [d for d in base_dir.iterdir() if d.is_dir() and "Stata" in d.name]
    if not stata_dirs:
        return None
    latest_dir = sorted(stata_dirs, key=lambda d: d.name, reverse=True)[0]
    return str(latest_dir)


def setup_env():
    from sfi import SFIToolkit  # type: ignore

    venv_path = _get_venv_path()
    uv_path = _get_uv_path()

    major, minor = sys.version_info.major, sys.version_info.minor
    site_packages = (
        venv_path / "Lib" / "site-packages"
        if sys.platform == "win32"
        else venv_path / "lib" / f"python{major}.{minor}" / "site-packages"
    )

    if str(site_packages) not in sys.path:
        sys.path.insert(0, str(site_packages))

    needs_setup = False
    try:
        import pyarrow
    except ImportError:
        needs_setup = True

    if needs_setup:
        try:
            SFIToolkit.displayln(
                f"dtparquet: setting up environment (Python {major}.{minor})..."
            )
            if venv_path.exists():
                import shutil

                shutil.rmtree(venv_path)

            subprocess.run(
                [uv_path, "venv", str(venv_path), "--python", f"{major}.{minor}"],
                check=True,
            )
            subprocess.run(
                [
                    uv_path,
                    "pip",
                    "install",
                    "--python",
                    str(venv_path),
                    "stata_setup",
                    "pyarrow",
                ],
                check=True,
            )
            SFIToolkit.displayln("dtparquet: setup complete.")
        except Exception as e:
            SFIToolkit.errprintln(f"dtparquet: setup failed: {str(e)}")
            raise


def stata_to_parquet(
    parquet_path: str, varlist: str = "", ifcond: str = "", inrange: str = ""
):
    from sfi import SFIToolkit  # type: ignore

    try:
        setup_env()
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        SFIToolkit.displayln("dtparquet: environment ready.")
    except Exception as e:
        SFIToolkit.errprintln(f"dtparquet error: {str(e)}")
        raise


def parquet_to_stata(parquet_path: str, varlist: str = "", clear: bool = False):
    from sfi import SFIToolkit  # type: ignore

    try:
        setup_env()
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        SFIToolkit.displayln("dtparquet: environment ready.")
    except Exception as e:
        SFIToolkit.errprintln(f"dtparquet error: {str(e)}")
        raise
