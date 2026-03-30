import datetime
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common

logger = logging.getLogger(__name__)

_RATE_LIMIT = 5
_RATE_WINDOW = 6 * 3600  # 6 hours in seconds
_MAX_COMPLAINT_LENGTH = 500


def _is_privileged(member):
    return any(
        r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
        for r in member.roles
    )


class ComplaintDeleteButton(discord.ui.Button):
    """A button that deletes a single complaint."""

    def __init__(self, complaint_id, guild_id):
        super().__init__(
            style=discord.ButtonStyle.danger,
            emoji='\N{WASTEBASKET}',
            custom_id=f'complaint_del:{complaint_id}',
        )
        self.complaint_id = complaint_id
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        if not _is_privileged(interaction.user):
            await interaction.response.send_message(
                'Only admins/moderators can remove complaints.', ephemeral=True
            )
            return

        complaint = cf_common.user_db.get_complaint(self.complaint_id)
        if complaint is None or str(complaint.guild_id) != str(self.guild_id):
            await interaction.response.send_message(
                f'Complaint #{self.complaint_id} not found.', ephemeral=True
            )
            return

        cf_common.user_db.delete_complaint(self.complaint_id)
        logger.info(
            f'Complaint #{self.complaint_id} removed by {interaction.user.id} '
            f'in guild {self.guild_id} (button)'
        )

        # Disable this button and update the message
        self.disabled = True
        self.style = discord.ButtonStyle.secondary
        await interaction.response.edit_message(view=self.view)
        await interaction.followup.send(
            embed=discord_common.embed_success(
                f'Complaint #{self.complaint_id} removed.'
            ),
            ephemeral=True,
        )


class ComplaintListView(discord.ui.View):
    """View for a complaint list page with optional delete buttons."""

    def __init__(self, complaint_ids, guild_id, show_buttons):
        super().__init__(timeout=300)
        if show_buttons:
            for cid in complaint_ids:
                self.add_item(ComplaintDeleteButton(cid, guild_id))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if hasattr(self, 'message') and self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class Complain(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.group(brief='Complaints', invoke_without_command=True)
    async def complain(self, ctx, *, text: str = None):
        """Add a complaint or list subcommands.

        Usage:
          ;complain <text>    — file a complaint
          ;complain list      — view all complaints
          ;complain remove <id or ids> — remove complaints (admin only)
        """
        if text is None:
            await ctx.send_help(ctx.command)
            return

        # Rate limit check for non-privileged users
        author = ctx.author
        if not _is_privileged(author):
            since = time.time() - _RATE_WINDOW
            count = cf_common.user_db.count_recent_complaints(
                ctx.guild.id, author.id, since
            )
            if count >= _RATE_LIMIT:
                await ctx.send(embed=discord_common.embed_alert(
                    f'Rate limit reached ({_RATE_LIMIT} complaints per 6 hours). '
                    'Please wait before filing another.'
                ))
                return

        if len(text) > _MAX_COMPLAINT_LENGTH:
            await ctx.send(embed=discord_common.embed_alert(
                f'Complaint too long (max {_MAX_COMPLAINT_LENGTH} characters).'
            ))
            return

        complaint_id = cf_common.user_db.add_complaint(
            ctx.guild.id, author.id, text
        )
        logger.info(f'Complaint #{complaint_id} added by {author.id} in guild {ctx.guild.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Complaint #{complaint_id} filed.'
        ))

    @complain.command(brief='List complaints')
    async def list(self, ctx):
        """View all complaints for this server.

        If invoked by an admin or moderator, delete buttons are shown next to
        each complaint.
        """
        complaints = cf_common.user_db.get_complaints(ctx.guild.id)
        if not complaints:
            await ctx.send(embed=discord_common.embed_neutral('No complaints filed.'))
            return

        show_buttons = _is_privileged(ctx.author)

        # Build pages. Each page holds complaints whose combined text fits in
        # an embed description (~3900 chars) and at most 25 buttons (Discord
        # limit per View is 25 components).
        max_per_page = 25 if show_buttons else None
        pages = []  # list of (description_text, [complaint_ids])
        current_lines = []
        current_ids = []
        length = 0
        for c in complaints:
            ts = datetime.datetime.fromtimestamp(c.created_at).strftime('%Y-%m-%d %H:%M')
            line = f'**#{c.id}** by <@{c.user_id}> ({ts})\n{c.text}'
            if (length + len(line) + 2 > 3900 and current_lines) or (
                max_per_page and len(current_ids) >= max_per_page
            ):
                pages.append(('\n\n'.join(current_lines), current_ids))
                current_lines = []
                current_ids = []
                length = 0
            current_lines.append(line)
            current_ids.append(c.id)
            length += len(line) + 2
        if current_lines:
            pages.append(('\n\n'.join(current_lines), current_ids))

        for i, (page_text, page_ids) in enumerate(pages):
            title = 'Complaints' if len(pages) == 1 else f'Complaints (page {i+1}/{len(pages)})'
            embed = discord.Embed(title=title, description=page_text, color=0xffaa10)
            view = ComplaintListView(page_ids, ctx.guild.id, show_buttons)
            msg = await ctx.send(embed=embed, view=view)
            view.message = msg

    @complain.command(brief='Remove complaint(s)', aliases=['delete'])
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def remove(self, ctx, *, ids: str):
        """Remove one or more complaints by ID. Admin/Moderator only.

        Usage:
          ;complain remove 5
          ;complain remove 1,2,3,4,5
        """
        # Parse comma/space-separated IDs
        raw_parts = ids.replace(',', ' ').split()
        parsed_ids = []
        for part in raw_parts:
            try:
                parsed_ids.append(int(part))
            except ValueError:
                await ctx.send(embed=discord_common.embed_alert(
                    f'Invalid complaint ID: `{part}`'
                ))
                return

        if not parsed_ids:
            await ctx.send(embed=discord_common.embed_alert('No complaint IDs provided.'))
            return

        if len(parsed_ids) == 1:
            # Single ID — use the original path for a specific not-found message
            complaint_id = parsed_ids[0]
            complaint = cf_common.user_db.get_complaint(complaint_id)
            if complaint is None or str(complaint.guild_id) != str(ctx.guild.id):
                await ctx.send(embed=discord_common.embed_alert(
                    f'Complaint #{complaint_id} not found.'
                ))
                return
            cf_common.user_db.delete_complaint(complaint_id)
            logger.info(f'Complaint #{complaint_id} removed by {ctx.author.id} in guild {ctx.guild.id}')
            await ctx.send(embed=discord_common.embed_success(
                f'Complaint #{complaint_id} removed.'
            ))
        else:
            # Bulk delete
            deleted = cf_common.user_db.delete_complaints(parsed_ids, ctx.guild.id)
            id_list = ', '.join(f'#{i}' for i in parsed_ids)
            logger.info(
                f'Bulk complaint removal by {ctx.author.id} in guild {ctx.guild.id}: '
                f'requested {id_list}, deleted {deleted}'
            )
            await ctx.send(embed=discord_common.embed_success(
                f'Removed {deleted} of {len(parsed_ids)} complaints.'
            ))


async def setup(bot):
    await bot.add_cog(Complain(bot))
