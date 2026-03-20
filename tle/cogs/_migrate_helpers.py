"""Helpers for the starboard migration cog.

Parsing old bot messages, embed serialization, and fallback message building.
"""
import json
import re

from tle.cogs.starboard import _starboard_content
from tle import constants

_OLD_BOT_RE = re.compile(
    r'^(.+?)\s+\*\*(\d+)\*\*\s*(?:·\s*.+?\s*)?\|\s*(https://discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+))'
)


def parse_old_bot_message(content):
    """Parse an old bot's starboard message content line.

    Expected formats:
        <emoji> **<count>** | <jump_url>
        <emoji> **<count>** · <author> | <jump_url>

    Returns (emoji_str, displayed_count, guild_id, channel_id, message_id) or None.
    All IDs are returned as ints.
    """
    if not content:
        return None
    match = _OLD_BOT_RE.match(content)
    if not match:
        return None
    emoji_str = match.group(1).strip()
    count = int(match.group(2))
    guild_id = int(match.group(4))
    channel_id = int(match.group(5))
    message_id = int(match.group(6))
    return emoji_str, count, guild_id, channel_id, message_id


def serialize_embed_fallback(message):
    """Serialize a Discord message's embeds + content to JSON for fallback rendering.

    Returns a JSON string containing the message content and embed data.
    """
    embeds = []
    for embed in message.embeds:
        data = {}
        if hasattr(embed, 'title') and embed.title:
            data['title'] = embed.title
        if hasattr(embed, 'description') and embed.description:
            data['description'] = embed.description
        if hasattr(embed, 'color') and embed.color is not None:
            data['color'] = int(embed.color)
        if hasattr(embed, 'image') and embed.image:
            url = getattr(embed.image, 'url', None)
            if url:
                data['image_url'] = url
        if hasattr(embed, 'author_data') and embed.author_data:
            data['author'] = embed.author_data
        elif hasattr(embed, 'author') and embed.author:
            author = embed.author
            data['author'] = {
                'name': getattr(author, 'name', None),
                'icon_url': getattr(author, 'icon_url', None),
                'url': getattr(author, 'url', None),
            }
        if hasattr(embed, 'fields') and embed.fields:
            fields = []
            for f in embed.fields:
                if isinstance(f, dict):
                    fields.append(f)
                else:
                    fields.append({
                        'name': getattr(f, 'name', None),
                        'value': getattr(f, 'value', None),
                        'inline': getattr(f, 'inline', True),
                    })
            if fields:
                data['fields'] = fields
        if hasattr(embed, 'footer') and embed.footer:
            if isinstance(embed.footer, dict):
                data['footer'] = embed.footer
            else:
                data['footer'] = {
                    'text': getattr(embed.footer, 'text', None),
                    'icon_url': getattr(embed.footer, 'icon_url', None),
                }
        if hasattr(embed, 'timestamp') and embed.timestamp:
            data['timestamp'] = str(embed.timestamp)
        if data:
            embeds.append(data)

    result = {}
    if hasattr(message, 'content') and message.content:
        result['content'] = message.content
    if embeds:
        result['embeds'] = embeds
    return json.dumps(result)


def build_fallback_message(entry, fallback_json, emoji_str):
    """Build a (content, embeds) tuple that copies the old bot's starboard post.

    The fallback JSON contains the old bot's original content line and embeds,
    serialized by serialize_embed_fallback(). This function reproduces them
    as faithfully as possible so the new post looks identical to the old one.

    entry: a DB row with .star_count, .original_msg_id, .guild_id, .source_channel_id
    fallback_json: JSON string from serialize_embed_fallback()
    emoji_str: the emoji string (used only if fallback has no content)
    """
    import discord

    data = {}
    if fallback_json:
        try:
            data = json.loads(fallback_json)
        except (json.JSONDecodeError, TypeError):
            data = {}

    # Use the old bot's original content line if available — it already has
    # the correct emoji, count, and jump URL. Only build our own if missing.
    content = data.get('content')
    if not content:
        count = entry.star_count if entry.star_count is not None else 0
        guild_id = getattr(entry, 'guild_id', None) or '0'
        channel_id = getattr(entry, 'source_channel_id', None) or '0'
        jump_url = f'https://discord.com/channels/{guild_id}/{channel_id}/{entry.original_msg_id}'
        content = _starboard_content(emoji_str, count, jump_url)

    # Rebuild each embed from the serialized data
    embeds = []
    for embed_data in data.get('embeds', []):
        embed = discord.Embed(
            title=embed_data.get('title'),
            description=embed_data.get('description'),
            color=embed_data.get('color', constants._DEFAULT_STAR_COLOR),
        )
        if embed_data.get('author'):
            author = embed_data['author']
            embed.set_author(
                name=author.get('name', 'Unknown'),
                icon_url=author.get('icon_url'),
                url=author.get('url'),
            )
        if embed_data.get('image_url'):
            embed.set_image(url=embed_data['image_url'])
        for field in embed_data.get('fields', []):
            embed.add_field(
                name=field.get('name', ''),
                value=field.get('value', ''),
                inline=field.get('inline', True),
            )
        if embed_data.get('footer'):
            footer = embed_data['footer']
            embed.set_footer(
                text=footer.get('text', ''),
                icon_url=footer.get('icon_url'),
            )
        embeds.append(embed)

    return content, embeds
