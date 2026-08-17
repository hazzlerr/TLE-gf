"""User database upgrades after 1.53.0."""

import logging

from tle.util.db._user_db_upgrade_registry import registry
from tle.util.db.counting_db import create_counting_schema


logger = logging.getLogger(__name__)


@registry.register('1.54.0', 'Persistent counting channels and attempt ledger')
def upgrade_1_54_0(db):
    """Add counting checkpoints and numeric-attempt audit rows."""
    logger.info('1.54.0: Adding counting channel state and attempt ledger')
    create_counting_schema(db)
    db.commit()
    logger.info('1.54.0: Upgrade complete')
