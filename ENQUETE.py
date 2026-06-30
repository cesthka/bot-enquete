import discord
from discord.ext import commands
import os
import sys
import json
import sqlite3
import logging
import traceback

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    sys.exit(1)

PREFIX = ";"

# Seuls ces IDs peuvent configurer l'embed (mêmes que BL / VM)
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
        "description": "Utilise les boutons ci-dessous pour configurer cet embed.",
        "color": DEFAULT_COLOR,
        "image": "",
        "thumbnail": "",
        "author_name": "",
        "author_icon": "",
        "footer_text": "",
        "footer_icon": "",
        "fields": [],          # [{name, value, inline}]
        "buttons": True,       # attacher les boutons profil à l'envoi
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
        desc = "*(embed vide — configure-le avec les boutons ci-dessous)*"

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
        em.add_field(
            name=(f.get("name") or "\u200b"),
            value=(f.get("value") or "\u200b"),
            inline=bool(f.get("inline")),
        )
    return em


def cfg_has_content(cfg):
    return bool(
        (cfg.get("title") or "").strip()
        or (cfg.get("description") or "").strip()
        or cfg.get("fields")
        or (cfg.get("image") or "").strip()
    )


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)


# ========================= PROFIL (boutons persistants) =========================

PROFILE_FIELDS = [
    ("prenom",   "Prénom / Pseudo",      False, 64),
    ("age",      "Âge",                  False, 8),
    ("genre",    "Genre / Pronoms",      False, 64),
    ("recherche", "Je recherche...",     False, 128),
    ("bio",      "Description / Bio",    True,  1000),
]


def build_profile_embed(user, data):
    em = discord.Embed(
        title=f"💌 Profil de {data.get('prenom') or user.display_name}",
        color=DEFAULT_COLOR,
    )
    try:
        em.set_thumbnail(url=user.display_avatar.url)
    except (AttributeError, TypeError):
        pass
    if data.get("age"):
        em.add_field(name="🎂 Âge", value=data["age"], inline=True)
    if data.get("genre"):
        em.add_field(name="🌸 Genre", value=data["genre"], inline=True)
    if data.get("recherche"):
        em.add_field(name="💞 Je recherche", value=data["recherche"], inline=False)
    if data.get("bio"):
        em.add_field(name="📝 À propos", value=data["bio"], inline=False)
    em.add_field(name="📨 Contact", value=user.mention, inline=False)
    return em


class ProfileModal(discord.ui.Modal):
    def __init__(self, existing=None):
        super().__init__(title="Mon profil 💌")
        existing = existing or {}
        self.inputs = {}
        for key, label, paragraph, maxlen in PROFILE_FIELDS:
            ti = discord.ui.TextInput(
                label=label,
                style=discord.TextStyle.paragraph if paragraph else discord.TextStyle.short,
                default=existing.get(key, ""),
                required=(key == "prenom"),
                max_length=maxlen,
            )
            self.inputs[key] = ti
            self.add_item(ti)

    async def on_submit(self, interaction: discord.Interaction):
        data = {key: self.inputs[key].value.strip() for key, *_ in PROFILE_FIELDS}
        set_profile(interaction.user.id, data)
        await interaction.response.send_message(
            embed=success_embed("✅ Profil enregistré", "Tu peux maintenant le publier avec **💞 Publier mon profil**."),
            ephemeral=True,
        )


class LoveProfileView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # vue persistante

    @discord.ui.button(label="Mon profil", emoji="💌", style=discord.ButtonStyle.secondary, custom_id="love_profile_edit")
    async def edit_profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        existing = get_profile(interaction.user.id)
        await interaction.response.send_modal(ProfileModal(existing=existing))

    @discord.ui.button(label="Publier mon profil", emoji="💞", style=discord.ButtonStyle.success, custom_id="love_profile_publish")
    async def publish_profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = get_profile(interaction.user.id)
        if not data or not data.get("prenom"):
            return await interaction.response.send_message(
                embed=error_embed("❌ Pas de profil", "Remplis d'abord ton profil avec **💌 Mon profil**."),
                ephemeral=True,
            )
        em = build_profile_embed(interaction.user, data)
        await interaction.response.send_message(embed=em)


# ========================= ÉDITEUR D'EMBED =========================

BUILDER_CONTENT = "🎨 **Éditeur d'embed** — configure avec les boutons, puis clique **Envoyer**."


# ---- Modals ----
class TitleModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Titre")
        self.builder = builder
        self.t = discord.ui.TextInput(label="Titre", default=builder.cfg.get("title", ""),
                                      required=False, max_length=256)
        self.u = discord.ui.TextInput(label="Lien du titre (optionnel)", default=builder.cfg.get("url", ""),
                                      required=False, max_length=512)
        self.add_item(self.t)
        self.add_item(self.u)

    async def on_submit(self, interaction):
        self.builder.cfg["title"] = self.t.value.strip()
        self.builder.cfg["url"] = self.u.value.strip()
        await self.builder.save_and_refresh(interaction)


class DescriptionModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Description")
        self.builder = builder
        self.d = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph,
                                      default=builder.cfg.get("description", ""), required=False, max_length=4000)
        self.add_item(self.d)

    async def on_submit(self, interaction):
        self.builder.cfg["description"] = self.d.value
        await self.builder.save_and_refresh(interaction)


class ColorModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Couleur")
        self.builder = builder
        cur = builder.cfg.get("color", DEFAULT_COLOR)
        self.c = discord.ui.TextInput(label="Couleur HEX (ex: E91E63)", default=f"{cur:06X}",
                                      required=True, max_length=7)
        self.add_item(self.c)

    async def on_submit(self, interaction):
        col = parse_color(self.c.value)
        if col is None:
            return await interaction.response.send_message("❌ Couleur HEX invalide (ex: `E91E63`).", ephemeral=True)
        self.builder.cfg["color"] = col
        await self.builder.save_and_refresh(interaction)


class AuthorModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Auteur")
        self.builder = builder
        self.n = discord.ui.TextInput(label="Nom de l'auteur", default=builder.cfg.get("author_name", ""),
                                      required=False, max_length=256)
        self.i = discord.ui.TextInput(label="URL de l'icône (optionnel)", default=builder.cfg.get("author_icon", ""),
                                      required=False, max_length=512)
        self.add_item(self.n)
        self.add_item(self.i)

    async def on_submit(self, interaction):
        self.builder.cfg["author_name"] = self.n.value.strip()
        self.builder.cfg["author_icon"] = self.i.value.strip()
        await self.builder.save_and_refresh(interaction)


class FooterModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Footer")
        self.builder = builder
        self.t = discord.ui.TextInput(label="Texte du footer", default=builder.cfg.get("footer_text", ""),
                                      required=False, max_length=2048)
        self.i = discord.ui.TextInput(label="URL de l'icône (optionnel)", default=builder.cfg.get("footer_icon", ""),
                                      required=False, max_length=512)
        self.add_item(self.t)
        self.add_item(self.i)

    async def on_submit(self, interaction):
        self.builder.cfg["footer_text"] = self.t.value.strip()
        self.builder.cfg["footer_icon"] = self.i.value.strip()
        await self.builder.save_and_refresh(interaction)


class ImageModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Image")
        self.builder = builder
        self.u = discord.ui.TextInput(label="URL de l'image (grande)", default=builder.cfg.get("image", ""),
                                      required=False, max_length=512)
        self.add_item(self.u)

    async def on_submit(self, interaction):
        self.builder.cfg["image"] = self.u.value.strip()
        await self.builder.save_and_refresh(interaction)


class ThumbnailModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Miniature")
        self.builder = builder
        self.u = discord.ui.TextInput(label="URL de la miniature (coin)", default=builder.cfg.get("thumbnail", ""),
                                      required=False, max_length=512)
        self.add_item(self.u)

    async def on_submit(self, interaction):
        self.builder.cfg["thumbnail"] = self.u.value.strip()
        await self.builder.save_and_refresh(interaction)


class FieldModal(discord.ui.Modal):
    def __init__(self, builder):
        super().__init__(title="Ajouter un champ")
        self.builder = builder
        self.n = discord.ui.TextInput(label="Nom du champ", required=True, max_length=256)
        self.v = discord.ui.TextInput(label="Valeur", style=discord.TextStyle.paragraph, required=True, max_length=1024)
        self.inline = discord.ui.TextInput(label="En ligne ? (oui / non)", default="non", required=False, max_length=4)
        self.add_item(self.n)
        self.add_item(self.v)
        self.add_item(self.inline)

    async def on_submit(self, interaction):
        inline = self.inline.value.strip().lower().startswith("o")
        self.builder.cfg.setdefault("fields", []).append({
            "name": self.n.value.strip(),
            "value": self.v.value.strip(),
            "inline": inline,
        })
        await self.builder.save_and_refresh(interaction)


# ---- Vue de l'éditeur ----
class EmbedBuilderView(discord.ui.View):
    def __init__(self, cfg, author_id):
        super().__init__(timeout=600)
        self.cfg = cfg
        self.author_id = author_id
        # Met à jour le label du toggle selon l'état
        for child in self.children:
            if isinstance(child, discord.ui.Button) and getattr(child, "custom_id", None) == "bld_toggle":
                child.label = f"Boutons profil : {'ON' if cfg.get('buttons', True) else 'OFF'}"

    async def interaction_check(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Ce menu n'est pas à toi.", ephemeral=True)
            return False
        return True

    async def save_and_refresh(self, interaction):
        set_embed_cfg(self.cfg)
        await interaction.response.edit_message(
            content=BUILDER_CONTENT,
            embed=build_embed_from_cfg(self.cfg, preview=True),
            view=EmbedBuilderView(self.cfg, self.author_id),
        )

    # Ligne 0
    @discord.ui.button(label="Titre", style=discord.ButtonStyle.primary, row=0)
    async def b_title(self, interaction, button):
        await interaction.response.send_modal(TitleModal(self))

    @discord.ui.button(label="Description", style=discord.ButtonStyle.primary, row=0)
    async def b_desc(self, interaction, button):
        await interaction.response.send_modal(DescriptionModal(self))

    @discord.ui.button(label="Couleur", style=discord.ButtonStyle.primary, row=0)
    async def b_color(self, interaction, button):
        await interaction.response.send_modal(ColorModal(self))

    @discord.ui.button(label="Auteur", style=discord.ButtonStyle.primary, row=0)
    async def b_author(self, interaction, button):
        await interaction.response.send_modal(AuthorModal(self))

    @discord.ui.button(label="Footer", style=discord.ButtonStyle.primary, row=0)
    async def b_footer(self, interaction, button):
        await interaction.response.send_modal(FooterModal(self))

    # Ligne 1
    @discord.ui.button(label="Image", style=discord.ButtonStyle.secondary, row=1)
    async def b_image(self, interaction, button):
        await interaction.response.send_modal(ImageModal(self))

    @discord.ui.button(label="Miniature", style=discord.ButtonStyle.secondary, row=1)
    async def b_thumb(self, interaction, button):
        await interaction.response.send_modal(ThumbnailModal(self))

    @discord.ui.button(label="Ajouter champ", style=discord.ButtonStyle.secondary, row=1)
    async def b_field(self, interaction, button):
        await interaction.response.send_modal(FieldModal(self))

    @discord.ui.button(label="Vider champs", style=discord.ButtonStyle.secondary, row=1)
    async def b_clearfields(self, interaction, button):
        self.cfg["fields"] = []
        await self.save_and_refresh(interaction)

    @discord.ui.button(label="Boutons profil : ON", style=discord.ButtonStyle.secondary, row=1, custom_id="bld_toggle")
    async def b_toggle(self, interaction, button):
        self.cfg["buttons"] = not self.cfg.get("buttons", True)
        await self.save_and_refresh(interaction)

    # Ligne 2
    @discord.ui.button(label="Envoyer", emoji="📤", style=discord.ButtonStyle.success, row=2)
    async def b_send(self, interaction, button):
        if not cfg_has_content(self.cfg):
            return await interaction.response.send_message(
                "❌ Ajoute au moins un **titre** ou une **description** avant d'envoyer.", ephemeral=True
            )
        em = build_embed_from_cfg(self.cfg, preview=False)
        view = LoveProfileView() if self.cfg.get("buttons", True) else None
        try:
            await interaction.channel.send(embed=em, view=view)
            await interaction.response.send_message("✅ Embed envoyé dans ce salon.", ephemeral=True)
        except discord.HTTPException as e:
            await interaction.response.send_message(
                f"❌ Échec de l'envoi (URL d'image invalide ?). Détail : `{e}`", ephemeral=True
            )

    @discord.ui.button(label="Réinitialiser", emoji="♻️", style=discord.ButtonStyle.danger, row=2)
    async def b_reset(self, interaction, button):
        self.cfg = default_embed_cfg()
        await self.save_and_refresh(interaction)


# ========================= COMMANDES =========================

@bot.command(name="embed")
async def _embed(ctx):
    if not is_owner(ctx.author.id):
        return  # Personne d'autre n'y a accès
    cfg = get_embed_cfg()
    await ctx.send(
        content=BUILDER_CONTENT,
        embed=build_embed_from_cfg(cfg, preview=True),
        view=EmbedBuilderView(cfg, ctx.author.id),
    )


@bot.command(name="help")
async def _help(ctx):
    if not is_owner(ctx.author.id):
        return
    em = discord.Embed(title="💗 Love — Aide", color=DEFAULT_COLOR)
    em.description = (
        f"`{PREFIX}embed` — Ouvrir l'éditeur d'embed (titre, description, couleur, image, champs...).\n"
        f"L'embed envoyé porte les boutons **💌 Mon profil** et **💞 Publier mon profil**."
    )
    await ctx.send(embed=em)


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    if not getattr(bot, "_views_added", False):
        bot.add_view(LoveProfileView())  # réactive les boutons des embeds déjà envoyés
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
