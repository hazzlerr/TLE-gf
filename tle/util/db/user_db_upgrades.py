"""
Database upgrade functions for user.db.
Register upgrades in version order; they run automatically on startup.
"""
import logging

from tle.util.db.upgrades import UpgradeRegistry

logger = logging.getLogger(__name__)

registry = UpgradeRegistry(version_table='db_version')


@registry.register('1.0.0', 'Baseline — no-op for existing schemas')
def upgrade_1_0_0(db):
    logger.info('1.0.0: Baseline upgrade (no-op)')
    pass


@registry.register('1.1.0', 'Multi-emoji starboard')
def upgrade_1_1_0(db):
    logger.info('1.1.0: Creating multi-emoji starboard v1 tables')
    # Create new v1 tables
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_config_v1 (
            guild_id    TEXT PRIMARY KEY,
            channel_id  TEXT
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
            guild_id    TEXT,
            emoji       TEXT,
            threshold   INTEGER NOT NULL DEFAULT 3,
            color       INTEGER NOT NULL DEFAULT 16755216,
            PRIMARY KEY (guild_id, emoji)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_message_v1 (
            original_msg_id     TEXT,
            starboard_msg_id    TEXT,
            guild_id            TEXT,
            emoji               TEXT,
            PRIMARY KEY (original_msg_id, emoji)
        )
    ''')

    # Migrate data from old starboard table → starboard_config_v1 + starboard_emoji_v1
    default_star = '\N{WHITE MEDIUM STAR}'
    default_threshold = 3
    default_color = 0xffaa10

    logger.info('1.1.0: Migrating old starboard config data...')
    try:
        rows = db.execute('SELECT guild_id, channel_id FROM starboard').fetchall()
        logger.info(f'1.1.0: Found {len(rows)} old starboard config rows to migrate')
        for row in rows:
            logger.debug(f'1.1.0: Migrating starboard config for guild {row.guild_id} '
                         f'(channel={row.channel_id})')
            db.execute(
                'INSERT OR IGNORE INTO starboard_config_v1 (guild_id, channel_id) VALUES (?, ?)',
                (row.guild_id, row.channel_id)
            )
            db.execute(
                'INSERT OR IGNORE INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) VALUES (?, ?, ?, ?)',
                (row.guild_id, default_star, default_threshold, default_color)
            )
        logger.info(f'1.1.0: Successfully migrated {len(rows)} starboard config entries')
    except Exception as e:
        logger.warning(f'1.1.0: Could not migrate old starboard config: {e}', exc_info=True)

    # Migrate old starboard_message → starboard_message_v1
    logger.info('1.1.0: Migrating old starboard message data...')
    try:
        rows = db.execute('SELECT original_msg_id, starboard_msg_id, guild_id FROM starboard_message').fetchall()
        logger.info(f'1.1.0: Found {len(rows)} old starboard message rows to migrate')
        for row in rows:
            logger.debug(f'1.1.0: Migrating starboard message {row.original_msg_id} '
                         f'(starboard_msg={row.starboard_msg_id}, guild={row.guild_id})')
            db.execute(
                'INSERT OR IGNORE INTO starboard_message_v1 (original_msg_id, starboard_msg_id, guild_id, emoji) '
                'VALUES (?, ?, ?, ?)',
                (row.original_msg_id, row.starboard_msg_id, row.guild_id, default_star)
            )
        logger.info(f'1.1.0: Successfully migrated {len(rows)} starboard message entries')
    except Exception as e:
        logger.warning(f'1.1.0: Could not migrate old starboard messages: {e}', exc_info=True)

    db.commit()


@registry.register('1.2.0', 'Guild config system')
def upgrade_1_2_0(db):
    logger.info('1.2.0: Creating guild_config table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS guild_config (
            guild_id    TEXT,
            key         TEXT,
            value       TEXT,
            PRIMARY KEY (guild_id, key)
        )
    ''')
    db.commit()
    logger.info('1.2.0: guild_config table created')


@registry.register('1.3.0', 'Star count and author tracking for leaderboards')
def upgrade_1_3_0(db):
    logger.info('1.3.0: Adding author_id and star_count columns to starboard_message_v1')
    # Add author_id column
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN author_id TEXT')
        logger.info('1.3.0: Added author_id column')
    except Exception as e:
        logger.debug(f'1.3.0: author_id column already exists or error: {e}')

    # Add star_count column
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN star_count INTEGER DEFAULT 0')
        logger.info('1.3.0: Added star_count column')
    except Exception as e:
        logger.debug(f'1.3.0: star_count column already exists or error: {e}')

    # Add channel_id column for backfill efficiency
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN channel_id TEXT')
        logger.info('1.3.0: Added channel_id column')
    except Exception as e:
        logger.debug(f'1.3.0: channel_id column already exists or error: {e}')

    db.commit()
    logger.info('1.3.0: Upgrade complete')
