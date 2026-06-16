"""Compatibility shim for the split DB-upgrade tests.

The DB upgrade-registry tests were split into ``test_migrations_path.py`` and
``test_migrations_versions.py`` (with shared helpers in
``migrations_test_utils.py``). This module re-exports the helpers so existing
imports such as ``from tests.test_migrations import make_registry_with_upgrades``
keep working. It contains no test functions of its own.
"""
from tests.migrations_test_utils import (  # noqa: F401
    db,
    make_registry_with_upgrades,
    create_legacy_tables,
)
