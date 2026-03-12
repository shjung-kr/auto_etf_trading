from __future__ import annotations

import argparse
from pathlib import Path
import sys
import unittest


def _build_suite(targets: list[str]) -> unittest.TestSuite:
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()

    effective_targets = targets or ["tests"]
    for target in effective_targets:
        path = Path(target)
        if path.is_dir():
            suite.addTests(loader.discover(start_dir=str(path)))
            continue
        if path.is_file():
            suite.addTests(
                loader.discover(
                    start_dir=str(path.parent or Path(".")),
                    pattern=path.name,
                )
            )
            continue
        suite.addTests(loader.loadTestsFromName(target))

    return suite


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pytest")
    parser.add_argument("targets", nargs="*")
    parser.add_argument("-q", "--quiet", action="store_true")
    arguments = parser.parse_args(argv)

    runner = unittest.TextTestRunner(verbosity=1 if arguments.quiet else 2)
    result = runner.run(_build_suite(arguments.targets))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
