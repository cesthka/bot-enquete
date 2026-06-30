import discord
from discord.ext import commands
import os
import sys
import json
import asyncio
import sqlite3
import logging
import traceback

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    sys.exit(1)

PREFIX = ";"

# Seuls ces IDs peuvent configurer le bot (mêmes que BL / VM)
OWNER_IDS = {923200874669563914, 142365250803466240}

DEFAULT_COLOR = 0xE91E63  # rose

DATA_DIR = os.environ.get("DATA_DIR")
if not DATA_DIR:
    print("[ERREUR CRITIQUE] DATA_DIR non défini. Configure DATA_DIR=/data dans Railway.")
    sys.exit(1)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "love_bot.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("love")


# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS profiles (user_id TEXT PRIMARY KEY, data TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS tickets (user_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL)")
    conn.commit()
    conn.close()


def get_config(key):
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()


# ---- Embed global ----
def default_embed_cfg():
    return {
        "title": "",
        "url": "",
        "description": "Utilise le menu déroulant ci-dessous pour configurer cet embed.",
        "color": DEFAULT_COLOR,
        "image": "",
        "thumbnail": "",
        "author_name": "",
        "author_icon": "",
        "footer_text": "",
        "footer_icon": "",
        "fields": [],
        "buttons": True,
    }


def get_embed_cfg():
    raw = get_config("love_embed")
    cfg = default_embed_cfg()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                cfg.update({k: data[k] for k in cfg if k in data})
        except (json.JSONDecodeError, TypeError):
            pass
    return cfg


def set_embed_cfg(cfg):
    set_config("love_embed", json.dumps(cfg))


# ---- Profils ----
def get_profile(user_id):
    conn = get_db()
    row = conn.execute("SELECT data FROM profiles WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row["data"])
    except (json.JSONDecodeError, TypeError):
        return None


def set_profile(user_id, data):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO profiles VALUES (?, ?)", (str(user_id), json.dumps(data)))
    conn.commit()
    conn.close()


# ---- Tickets ----
def set_ticket(user_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO tickets VALUES (?, ?)", (str(user_id), str(channel_id)))
    conn.commit()
    conn.close()


def get_ticket_channel(user_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM tickets WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def get_ticket_owner(channel_id):
    conn = get_db()
    row = conn.execute("SELECT user_id FROM tickets WHERE channel_id = ?", (str(channel_id),)).fetchone()
    conn.close()
    return int(row["user_id"]) if row else None


def remove_ticket_by_channel(channel_id):
    conn = get_db()
    conn.execute("DELETE FROM tickets WHERE channel_id = ?", (str(channel_id),))
    conn.commit()
    conn.close()


def remove_ticket_by_user(user_id):
    conn = get_db()
    conn.execute("DELETE FROM tickets WHERE user_id = ?", (str(user_id),))
    conn.commit()
    conn.close()


# ========================= HELPERS =========================

def is_owner(user_id):
    return user_id in OWNER_IDS


def parse_color(s):
    if not s:
        return None
    s = s.strip().lstrip("#")
    try:
        return int(s, 16) & 0xFFFFFF
    except ValueError:
        return None


def error_embed(title, desc=""):
    return discord.Embed(title=title, description=desc, color=0xf04747)


def success_embed(title, desc=""):
    return discord.Embed(title=title, description=desc, color=0x43b581)


def build_embed_from_cfg(cfg, preview=False):
    title = (cfg.get("title") or "").strip()
    desc = (cfg.get("description") or "").strip()
    if preview and not (title or desc or cfg.get("fields") or cfg.get("image")):
        desc = "*(embed vide — configure-le avec le menu ci-dessous)*"

    em = discord.Embed(color=cfg.get("color", DEFAULT_COLOR))
    if title:
        em.title = title
        if (cfg.get("url") or "").strip():
            em.url = cfg["url"].strip()
    if desc:
        em.description = desc
    if (cfg.get("author_name") or "").strip():
        em.set_author(name=cfg["author_name"].strip(), icon_url=(cfg.get("author_icon") or None) or None)
    if (cfg.get("footer_text") or "").strip():
        em.set_footer(text=cfg["footer_text"].strip(), icon_url=(cfg.get("footer_icon") or None) or None)
    if (cfg.get("image") or "").strip():
        em.set_image(url=cfg["image"].strip())
    if (cfg.get("thumbnail") or "").strip():
        em.set_thumbnail(url=cfg["thumbnail"].strip())
    for f in cfg.get("fields", []):
        em.add_field(name=(f.get("name") or "\u200b"), value=(f.get("value") or "\u200b"), inline=bool(f.get("inline")))
    return em


def cfg_has_content(cfg):
    return bool(
        (cfg.get("title") or "").strip()
        or (cfg.get("description") or "").strip()
        or cfg.get("fields")
        or (cfg.get("image") or "").strip()
    )


def genre_label(data):
    g = data.get("genre")
    if g == "homme":
        return "Homme ♂️"
    if g == "femme":
        return "Femme ♀️"
    if g == "autre":
        return f"Autre — {data.get('autre_detail') or 'non précisé'}"
    return None


def build_profile_embed(user, data, published=False):
    em = discord.Embed(title=f"💌 {data.get('prenom') or user.display_name}", color=DEFAULT_COLOR)
    try:
        em.set_thumbnail(url=user.display_avatar.url)
    except (AttributeError, TypeError):
        pass

    def fld(name, value, inline=True):
        if value:
            em.add_field(name=name, value=value, inline=inline)
        elif not published:
            em.add_field(name=name, value="*(non rempli)*", inline=inline)

    fld("🎂 Âge", data.get("age"))
    fld("🌸 Genre", genre_label(data))
    fld("💞 Je recherche", data.get("recherche"), inline=False)
    fld("📝 À propos", data.get("bio"), inline=False)
    em.add_field(name="📨 Contact", value=user.mention, inline=False)

    if not published:
        em.set_footer(text="Édite chaque élément via le menu, puis choisis « Publier mon profil ».")
    return em


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)


# ========================= SAISIE PAR MESSAGE =========================

async def channel_prompt_and_wait(channel, author_id, prompt, timeout=120):
    """Envoie une question dans le salon, attend la réponse de author_id, supprime les 2 messages, renvoie le texte."""
    try:
        msg = await channel.send(prompt)
    except discord.HTTPException:
        return None

    def check(m):
        return m.author.id == author_id and m.channel.id == channel.id

    try:
        reply = await bot.wait_for("message", check=check, timeout=timeout)
    except asyncio.TimeoutError:
        try:
            await msg.edit(content="⏱️ Temps écoulé.")
        except discord.HTTPException:
            pass
        return None

    text = reply.content
    try:
        await reply.delete()
    except discord.HTTPException:
        pass
    try:
        await msg.delete()
    except discord.HTTPException:
        pass
    return text


# ========================= PUBLICATION =========================

async def do_publish(guild, user, data):
    if not data or not data.get("prenom"):
        return False, "❌ Ton profil est incomplet (prénom manquant). Édite-le d'abord."
    genre = data.get("genre")
    if genre not in ("homme", "femme", "autre"):
        return False, "❌ Choisis ton **genre** dans ton profil avant de publier."

    key = {"homme": "channel_homme", "femme": "channel_femme", "autre": "channel_autre"}[genre]
    cid = get_config(key)
    if not cid:
        return False, f"❌ Le salon pour « {genre} » n'est pas configuré. Préviens un admin."
    ch = guild.get_channel(int(cid))
    if not ch:
        return False, "❌ Le salon de publication configuré est introuvable."

    try:
        await ch.send(embed=build_profile_embed(user, data, published=True))
    except discord.HTTPException as e:
        return False, f"❌ Échec de la publication : `{e}`"
    return True, f"✅ Ton profil a été publié dans {ch.mention} !"


# ========================= TICKET PROFIL =========================

async def open_profile_ticket(interaction):
    guild = interaction.guild
    user = interaction.user

    cat_id = get_config("ticket_category")
    if not cat_id:
        return await interaction.followup.send(
            embed=error_embed("⚙️ Pas configuré", "Le système de profils n'est pas encore configuré. Préviens un admin."),
            ephemeral=True,
        )
    category = guild.get_channel(int(cat_id))
    if not isinstance(category, discord.CategoryChannel):
        return await interaction.followup.send(
            embed=error_embed("⚙️ Catégorie introuvable", "La catégorie configurée n'existe plus."),
            ephemeral=True,
        )

    existing = get_ticket_channel(user.id)
    if existing:
        ch = guild.get_channel(int(existing))
        if ch:
            return await interaction.followup.send(f"Tu as déjà un espace profil : {ch.mention}", ephemeral=True)
        remove_ticket_by_user(user.id)

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True, manage_channels=True),
    }
    for oid in OWNER_IDS:
        m = guild.get_member(oid)
        if m:
            overwrites[m] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

    try:
        ch = await category.create_text_channel(name=f"profil-{user.name}", overwrites=overwrites)
    except discord.Forbidden:
        return await interaction.followup.send(
            embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission **Gérer les salons**."),
            ephemeral=True,
        )

    set_ticket(user.id, ch.id)
    data = get_profile(user.id) or {}
    await ch.send(
        content=f"{user.mention} bienvenue dans ton espace profil 💌",
        embed=build_profile_embed(user, data, published=False),
        view=TicketView(),
    )
    await interaction.followup.send(f"✅ Ton espace profil a été créé : {ch.mention}", ephemeral=True)


class TicketEditSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Prénom / Pseudo", value="prenom", emoji="✏️"),
            discord.SelectOption(label="Âge", value="age", emoji="🎂"),
            discord.SelectOption(label="Genre", value="genre", emoji="🌸", description="Homme / Femme / Autre"),
            discord.SelectOption(label="Je recherche", value="recherche", emoji="💞"),
            discord.SelectOption(label="Description / Bio", value="bio", emoji="📝"),
            discord.SelectOption(label="Publier mon profil", value="publish", emoji="💌"),
            discord.SelectOption(label="Fermer le ticket", value="close", emoji="🔒"),
        ]
        super().__init__(placeholder="Modifier / publier ton profil...", min_values=1, max_values=1,
                         options=options, custom_id="ticket_edit_select")

    async def callback(self, interaction: discord.Interaction):
        owner_id = get_ticket_owner(interaction.channel.id)
        if owner_id is None:
            return await interaction.response.send_message("Espace profil introuvable.", ephemeral=True)
        if interaction.user.id != owner_id and not is_owner(interaction.user.id):
            return await interaction.response.send_message("Ce profil n'est pas le tien.", ephemeral=True)

        val = self.values[0]
        user = interaction.guild.get_member(owner_id) or interaction.user
        data = get_profile(owner_id) or {}

        if val == "close":
            await interaction.response.send_message("🔒 Fermeture de ton espace profil dans 3 secondes...")
            remove_ticket_by_channel(interaction.channel.id)
            await asyncio.sleep(3)
            try:
                await interaction.channel.delete()
            except discord.HTTPException:
                pass
            return

        await interaction.response.defer()
        ch = interaction.channel
        msg = interaction.message

        async def refresh():
            try:
                await msg.edit(embed=build_profile_embed(user, data, published=False), view=TicketView())
            except discord.HTTPException:
                pass

        if val == "publish":
            ok, m = await do_publish(interaction.guild, user, data)
            await interaction.followup.send(m, ephemeral=True)
            await refresh()
            return

        if val == "genre":
            text = await channel_prompt_and_wait(ch, owner_id, "Quel est ton **genre** ? Réponds **Homme**, **Femme** ou **Autre**.")
            if text is None:
                return
            g = text.strip().lower()
            if g.startswith("h"):
                data["genre"] = "homme"
                data.pop("autre_detail", None)
            elif g.startswith("f"):
                data["genre"] = "femme"
                data.pop("autre_detail", None)
            elif g.startswith("a"):
                data["genre"] = "autre"
                det = await channel_prompt_and_wait(ch, owner_id, "Précise ton genre (texte libre) :")
                data["autre_detail"] = (det.strip() if det else "")
            else:
                await interaction.followup.send("❌ Réponds **Homme**, **Femme** ou **Autre**.", ephemeral=True)
                return
            set_profile(owner_id, data)
            await refresh()
            return

        prompts = {
            "prenom": "Envoie ton **prénom / pseudo**.",
            "age": "Envoie ton **âge**.",
            "recherche": "Envoie ce que tu **recherches**.",
            "bio": "Envoie ta **description / bio**.",
        }
        if val in prompts:
            text = await channel_prompt_and_wait(ch, owner_id, prompts[val])
            if text is None:
                return
            data[val] = text.strip()
            set_profile(owner_id, data)
            await refresh()


class TicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketEditSelect())


# ========================= EMBED PUBLIC (boutons persistants) =========================

class LoveProfileView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Mon profil", emoji="💌", style=discord.ButtonStyle.secondary, custom_id="love_profile_edit")
    async def edit_profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await open_profile_ticket(interaction)

    @discord.ui.button(label="Publier mon profil", emoji="💞", style=discord.ButtonStyle.success, custom_id="love_profile_publish")
    async def publish_profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        data = get_profile(interaction.user.id)
        ok, m = await do_publish(interaction.guild, interaction.user, data or {})
        await interaction.followup.send(m, ephemeral=True)


# ========================= ÉDITEUR D'EMBED (menu déroulant) =========================

BUILDER_CONTENT = "🎨 **Éditeur d'embed** — choisis un élément dans le menu, je te demanderai sa valeur par message."


class EmbedSelect(discord.ui.Select):
    def __init__(self, cfg):
        toggle_state = "ON" if cfg.get("buttons", True) else "OFF"
        options = [
            discord.SelectOption(label="Titre", value="title", emoji="🔠"),
            discord.SelectOption(label="Description", value="description", emoji="📝"),
            discord.SelectOption(label="Couleur", value="color", emoji="🎨"),
            discord.SelectOption(label="Image", value="image", emoji="🖼️"),
            discord.SelectOption(label="Miniature", value="thumbnail", emoji="🏞️"),
            discord.SelectOption(label="Auteur", value="author", emoji="👤"),
            discord.SelectOption(label="Footer", value="footer", emoji="🦶"),
            discord.SelectOption(label="Ajouter un champ", value="addfield", emoji="➕"),
            discord.SelectOption(label="Vider les champs", value="clearfields", emoji="🗑️"),
            discord.SelectOption(label=f"Boutons profil : {toggle_state}", value="toggle", emoji="🔘"),
            discord.SelectOption(label="Envoyer l'embed", value="send", emoji="📤"),
            discord.SelectOption(label="Réinitialiser", value="reset", emoji="♻️"),
        ]
        super().__init__(placeholder="Que veux-tu modifier ?", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        cfg = view.cfg
        aid = view.author_id
        val = self.values[0]

        await interaction.response.defer()
        ch = interaction.channel
        msg = interaction.message

        async def refresh():
            set_embed_cfg(view.cfg)
            try:
                await msg.edit(content=BUILDER_CONTENT, embed=build_embed_from_cfg(view.cfg, preview=True),
                               view=EmbedBuilderView(view.cfg, aid))
            except discord.HTTPException:
                pass

        if val == "send":
            if not cfg_has_content(cfg):
                await interaction.followup.send("❌ Ajoute au moins un **titre** ou une **description** d'abord.", ephemeral=True)
                await refresh()
                return
            em = build_embed_from_cfg(cfg, preview=False)
            pub_view = LoveProfileView() if cfg.get("buttons", True) else None
            try:
                await ch.send(embed=em, view=pub_view)
                await interaction.followup.send("✅ Embed envoyé dans ce salon.", ephemeral=True)
            except discord.HTTPException as e:
                await interaction.followup.send(f"❌ Échec de l'envoi : `{e}`", ephemeral=True)
            await refresh()
            return

        if val == "reset":
            view.cfg = default_embed_cfg()
            await refresh()
            return

        if val == "toggle":
            cfg["buttons"] = not cfg.get("buttons", True)
            await refresh()
            return

        if val == "clearfields":
            cfg["fields"] = []
            await refresh()
            return

        if val == "addfield":
            text = await channel_prompt_and_wait(ch, aid, "Envoie le champ au format : `nom | valeur | oui`  *(oui/non = en ligne)*")
            if text is None:
                return
            parts = [p.strip() for p in text.split("|")]
            name = parts[0] if parts else ""
            value = parts[1] if len(parts) > 1 else ""
            inline = len(parts) > 2 and parts[2].lower().startswith("o")
            if name and value:
                cfg.setdefault("fields", []).append({"name": name, "value": value, "inline": inline})
            await refresh()
            return

        prompts = {
            "title": "Envoie le **titre**. *(`vide` pour effacer)*",
            "description": "Envoie la **description**. *(`vide` pour effacer)*",
            "color": "Envoie une **couleur HEX** (ex: `E91E63`).",
            "image": "Envoie l'**URL de l'image**. *(`vide` pour effacer)*",
            "thumbnail": "Envoie l'**URL de la miniature**. *(`vide` pour effacer)*",
            "author": "Envoie l'auteur au format : `nom | url_icone`  *(icône optionnelle)*",
            "footer": "Envoie le footer au format : `texte | url_icone`  *(icône optionnelle)*",
        }
        if val in prompts:
            text = await channel_prompt_and_wait(ch, aid, prompts[val])
            if text is None:
                return
            v = text.strip()
            low = v.lower()
            if val == "color":
                col = parse_color(v)
                if col is None:
                    await interaction.followup.send("❌ Couleur HEX invalide (ex: `E91E63`).", ephemeral=True)
                    await refresh()
                    return
                cfg["color"] = col
            elif val == "author":
                parts = [p.strip() for p in v.split("|")]
                cfg["author_name"] = parts[0]
                cfg["author_icon"] = parts[1] if len(parts) > 1 else ""
            elif val == "footer":
                parts = [p.strip() for p in v.split("|")]
                cfg["footer_text"] = parts[0]
                cfg["footer_icon"] = parts[1] if len(parts) > 1 else ""
            else:
                if low in ("vide", "clear", "none", "-"):
                    v = ""
                cfg[val] = v
            await refresh()


class EmbedBuilderView(discord.ui.View):
    def __init__(self, cfg, author_id):
        super().__init__(timeout=600)
        self.cfg = cfg
        self.author_id = author_id
        self.add_item(EmbedSelect(cfg))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Ce menu n'est pas à toi.", ephemeral=True)
            return False
        return True


# ========================= CONFIG (salons & catégorie) =========================

def resolve_text_channel(guild, text):
    text = text.strip()
    cleaned = text.strip("<#>")
    try:
        cid = int(cleaned)
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch
    except ValueError:
        pass
    name = text.lstrip("#").lower()
    for ch in guild.text_channels:
        if ch.name.lower() == name:
            return ch
    return None


def resolve_category(guild, text):
    text = text.strip()
    try:
        cid = int(text)
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.CategoryChannel):
            return ch
    except ValueError:
        pass
    for cat in guild.categories:
        if cat.name.lower() == text.lower():
            return cat
    return None


def build_config_embed(guild):
    def show(key):
        cid = get_config(key)
        if not cid:
            return "*non défini*"
        ch = guild.get_channel(int(cid))
        return ch.mention if ch else f"`{cid}` *(introuvable)*"

    em = discord.Embed(
        title="⚙️ Configuration Love",
        description="Choisis un élément dans le menu pour le configurer.",
        color=DEFAULT_COLOR,
    )
    em.add_field(name="📁 Catégorie des tickets", value=show("ticket_category"), inline=False)
    em.add_field(name="♂️ Salon Homme", value=show("channel_homme"), inline=False)
    em.add_field(name="♀️ Salon Femme", value=show("channel_femme"), inline=False)
    em.add_field(name="⚧️ Salon Autre", value=show("channel_autre"), inline=False)
    return em


class ConfigSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Catégorie des tickets", value="ticket_category", emoji="📁"),
            discord.SelectOption(label="Salon Homme", value="channel_homme", emoji="♂️"),
            discord.SelectOption(label="Salon Femme", value="channel_femme", emoji="♀️"),
            discord.SelectOption(label="Salon Autre", value="channel_autre", emoji="⚧️"),
        ]
        super().__init__(placeholder="Que veux-tu configurer ?", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        val = self.values[0]
        aid = self.view.author_id
        await interaction.response.defer()
        ch = interaction.channel
        msg = interaction.message
        guild = interaction.guild

        if val == "ticket_category":
            text = await channel_prompt_and_wait(ch, aid, "Envoie l'**ID** ou le **nom exact** de la catégorie pour les tickets.")
            if text is None:
                return
            cat = resolve_category(guild, text)
            if not cat:
                await interaction.followup.send("❌ Catégorie introuvable.", ephemeral=True)
            else:
                set_config("ticket_category", cat.id)
        else:
            label = {"channel_homme": "Homme", "channel_femme": "Femme", "channel_autre": "Autre"}[val]
            text = await channel_prompt_and_wait(ch, aid, f"Mentionne le **salon {label}** (#salon) ou envoie son **ID**.")
            if text is None:
                return
            target = resolve_text_channel(guild, text)
            if not target:
                await interaction.followup.send("❌ Salon introuvable.", ephemeral=True)
            else:
                set_config(val, target.id)

        try:
            await msg.edit(embed=build_config_embed(guild), view=ConfigView(aid))
        except discord.HTTPException:
            pass


class ConfigView(discord.ui.View):
    def __init__(self, author_id):
        super().__init__(timeout=600)
        self.author_id = author_id
        self.add_item(ConfigSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not is_owner(interaction.user.id):
            await interaction.response.send_message("Tu n'as pas accès à ça.", ephemeral=True)
            return False
        return True


# ========================= COMMANDES =========================

@bot.command(name="embed")
async def _embed(ctx):
    if not is_owner(ctx.author.id):
        return
    cfg = get_embed_cfg()
    await ctx.send(content=BUILDER_CONTENT, embed=build_embed_from_cfg(cfg, preview=True),
                   view=EmbedBuilderView(cfg, ctx.author.id))


@bot.command(name="config")
async def _config(ctx):
    if not is_owner(ctx.author.id):
        return
    await ctx.send(embed=build_config_embed(ctx.guild), view=ConfigView(ctx.author.id))


@bot.command(name="help")
async def _help(ctx):
    if not is_owner(ctx.author.id):
        return
    em = discord.Embed(title="💗 Love — Aide", color=DEFAULT_COLOR)
    em.description = (
        f"`{PREFIX}embed` — Éditeur de l'embed (menu déroulant + saisie par message).\n"
        f"`{PREFIX}config` — Régler la catégorie des tickets et les salons Homme / Femme / Autre.\n\n"
        f"L'embed envoyé porte les boutons **💌 Mon profil** (crée un ticket) et "
        f"**💞 Publier mon profil** (publie dans le salon du genre)."
    )
    await ctx.send(embed=em)


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    if not getattr(bot, "_views_added", False):
        bot.add_view(LoveProfileView())
        bot.add_view(TicketView())
        bot._views_added = True
    log.info(f"Bot connecté : {bot.user} ({bot.user.id})")
    log.info(f"Prefix : {PREFIX}")


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandInvokeError):
        error = error.original
    if isinstance(error, commands.CommandNotFound):
        return
    log.error(
        f"Erreur '{ctx.command}' par {ctx.author} : {error}\n"
        + "".join(traceback.format_exception(type(error), error, error.__traceback__))
    )


# ========================= RUN =========================
if __name__ == "__main__":
    try:
        log.info("Démarrage du bot Love...")
        bot.run(BOT_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur fatale au démarrage : {e}", exc_info=True)
        sys.exit(1)
