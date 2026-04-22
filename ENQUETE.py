"""
╔══════════════════════════════════════════════════════════════════════════╗
║                          ENQUÊTE — Bot Meira                             ║
║  Jeu social de déduction à rôles cachés. Ambiance japonaise subtile.     ║
║  Structure : enquête hybride avec actions secrètes + débat + vote.       ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
import discord
from discord.ext import commands, tasks
import os
import sys
import sqlite3
import json
import random
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN_ENQUETE") or os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] Aucune variable d'environnement TOKEN_ENQUETE ni TOKEN trouvée.")
    print("Définis-la avant de lancer le bot (ex: export TOKEN_ENQUETE=xxx).")
    sys.exit(1)

PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [1312375517927706630]  # Même buyer que Velda par défaut, modifiable
DEFAULT_PREFIX = "!"
DB_PATH = "enquete.db"

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("enquete")

# Verrou global pour les opérations stats critiques
stats_lock = asyncio.Lock()

# Cache du prefix
_prefix_cache = {"value": None}


# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # Config globale (prefix, buyer_ids, etc.)
    c.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY, value TEXT
    )""")

    # Rangs utilisateurs (Buyer 4 > Sys 3 > MJ 2 > Joueur vérifié 1 > Aucun 0)
    c.execute("""CREATE TABLE IF NOT EXISTS ranks (
        user_id TEXT PRIMARY KEY, rank INTEGER NOT NULL
    )""")

    # Ban du bot
    c.execute("""CREATE TABLE IF NOT EXISTS bot_bans (
        user_id TEXT PRIMARY KEY,
        banned_by TEXT,
        banned_at TEXT
    )""")

    # Salons de log
    c.execute("""CREATE TABLE IF NOT EXISTS log_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    # Salons autorisés (Sys+ bypass)
    c.execute("""CREATE TABLE IF NOT EXISTS allowed_channels (
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        added_by TEXT,
        added_at TEXT,
        PRIMARY KEY (guild_id, channel_id)
    )""")

    # Tracking des messages actifs (pour sélection random si besoin)
    c.execute("""CREATE TABLE IF NOT EXISTS active_messages (
        guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        timestamp TEXT,
        PRIMARY KEY (guild_id, user_id)
    )""")

    # Stats par joueur (XP, niveau, classe, parties jouées/gagnées...)
    c.execute("""CREATE TABLE IF NOT EXISTS player_stats (
        user_id TEXT PRIMARY KEY,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 0,
        games_played INTEGER DEFAULT 0,
        games_won INTEGER DEFAULT 0,
        times_culprit INTEGER DEFAULT 0,
        culprit_wins INTEGER DEFAULT 0,
        times_detective INTEGER DEFAULT 0,
        correct_accusations INTEGER DEFAULT 0,
        wrong_accusations INTEGER DEFAULT 0,
        favorite_role TEXT,
        last_played TEXT
    )""")

    # Compteurs par rôle joué (pour "rôle préféré" et badges)
    c.execute("""CREATE TABLE IF NOT EXISTS role_counts (
        user_id TEXT NOT NULL,
        role_key TEXT NOT NULL,
        count INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, role_key)
    )""")

    # Badges débloqués
    c.execute("""CREATE TABLE IF NOT EXISTS badges (
        user_id TEXT NOT NULL,
        badge_key TEXT NOT NULL,
        unlocked_at TEXT,
        PRIMARY KEY (user_id, badge_key)
    )""")

    # Historique des parties (pour replay/stats)
    c.execute("""CREATE TABLE IF NOT EXISTS game_history (
        game_id TEXT PRIMARY KEY,
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        host_id TEXT,
        scenario_key TEXT,
        size_mode TEXT,
        player_count INTEGER,
        culprit_id TEXT,
        culprit_survived INTEGER,
        started_at TEXT,
        ended_at TEXT,
        participants_json TEXT
    )""")

    # Cooldowns (pour empêcher les spams de commandes coûteuses)
    c.execute("""CREATE TABLE IF NOT EXISTS cooldowns (
        user_id TEXT NOT NULL,
        key TEXT NOT NULL,
        until TEXT NOT NULL,
        PRIMARY KEY (user_id, key)
    )""")

    # Valeurs par défaut
    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute(
        "INSERT OR IGNORE INTO config VALUES ('buyer_ids', ?)",
        (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),)
    )

    conn.commit()
    conn.close()


# ---- Config ----

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
    if key == "prefix":
        _prefix_cache["value"] = str(value)


def get_prefix_cached():
    if _prefix_cache["value"] is None:
        _prefix_cache["value"] = get_config("prefix") or DEFAULT_PREFIX
    return _prefix_cache["value"]


# ---- Rangs ----

def get_rank_db(user_id):
    buyer_ids_raw = get_config("buyer_ids")
    if buyer_ids_raw:
        buyer_ids = json.loads(buyer_ids_raw)
        if str(user_id) in buyer_ids:
            return 4
    conn = get_db()
    row = conn.execute("SELECT rank FROM ranks WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row["rank"] if row else 0


def set_rank_db(user_id, rank):
    conn = get_db()
    if rank == 0:
        conn.execute("DELETE FROM ranks WHERE user_id = ?", (str(user_id),))
    else:
        conn.execute("INSERT OR REPLACE INTO ranks VALUES (?, ?)", (str(user_id), rank))
    conn.commit()
    conn.close()


def get_ranks_by_level(level):
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM ranks WHERE rank = ?", (level,)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


def has_min_rank(user_id, minimum):
    return get_rank_db(user_id) >= minimum


def rank_name(level):
    return {4: "Buyer", 3: "Sys", 2: "MJ", 1: "Joueur vérifié", 0: "Aucun"}[level]


# ---- Ban bot ----

def is_bot_banned(user_id):
    conn = get_db()
    row = conn.execute("SELECT 1 FROM bot_bans WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row is not None


def add_bot_ban(user_id, banned_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).strftime("%d/%m/%Y %Hh%M")
    conn.execute(
        "INSERT OR REPLACE INTO bot_bans VALUES (?, ?, ?)",
        (str(user_id), str(banned_by), now)
    )
    conn.commit()
    conn.close()


def remove_bot_ban(user_id):
    conn = get_db()
    conn.execute("DELETE FROM bot_bans WHERE user_id = ?", (str(user_id),))
    conn.commit()
    conn.close()


# ---- Log channels ----

def get_log_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM log_channels WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def set_log_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO log_channels VALUES (?, ?)",
        (str(guild_id), str(channel_id))
    )
    conn.commit()
    conn.close()


# ---- Allowed channels ----

def add_allowed_channel(guild_id, channel_id, added_by):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO allowed_channels (guild_id, channel_id, added_by, added_at) VALUES (?, ?, ?, ?)",
        (str(guild_id), str(channel_id), str(added_by), now)
    )
    conn.commit()
    conn.close()


def remove_allowed_channel(guild_id, channel_id):
    conn = get_db()
    cur = conn.execute(
        "DELETE FROM allowed_channels WHERE guild_id = ? AND channel_id = ?",
        (str(guild_id), str(channel_id))
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted > 0


def get_allowed_channels(guild_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT channel_id FROM allowed_channels WHERE guild_id = ?",
        (str(guild_id),)
    ).fetchall()
    conn.close()
    return [r["channel_id"] for r in rows]


def is_channel_allowed(guild_id, channel_id):
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM allowed_channels WHERE guild_id = ? AND channel_id = ? LIMIT 1",
        (str(guild_id), str(channel_id))
    ).fetchone()
    conn.close()
    return row is not None


# ---- Tracking messages actifs ----

def track_message(guild_id, user_id):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO active_messages (guild_id, user_id, timestamp) VALUES (?, ?, ?)",
        (str(guild_id), str(user_id), now)
    )
    conn.commit()
    conn.close()


def get_active_members(guild_id, hours=24, limit=50):
    conn = get_db()
    cutoff = (datetime.now(PARIS_TZ) - timedelta(hours=hours)).isoformat()
    rows = conn.execute("""SELECT user_id FROM active_messages
        WHERE guild_id = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT ?""",
        (str(guild_id), cutoff, limit)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


# ---- Player stats ----

def get_player_stats(user_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM player_stats WHERE user_id = ?", (str(user_id),)).fetchone()
    if not row:
        conn.execute("INSERT OR IGNORE INTO player_stats (user_id) VALUES (?)", (str(user_id),))
        conn.commit()
        row = conn.execute("SELECT * FROM player_stats WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return dict(row)


def update_player_stats(user_id, **kwargs):
    """Met à jour des champs du profil joueur atomiquement."""
    get_player_stats(user_id)  # s'assure que la ligne existe
    if not kwargs:
        return
    set_clauses = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [str(user_id)]
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"UPDATE player_stats SET {set_clauses} WHERE user_id = ?", values)
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"update_player_stats failed: {e}")
    finally:
        conn.close()


def increment_player_stat(user_id, field, delta=1):
    get_player_stats(user_id)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            f"UPDATE player_stats SET {field} = {field} + ? WHERE user_id = ?",
            (delta, str(user_id))
        )
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"increment_player_stat failed: {e}")
    finally:
        conn.close()


def increment_role_count(user_id, role_key):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""INSERT INTO role_counts (user_id, role_key, count)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id, role_key) DO UPDATE SET count = count + 1""",
            (str(user_id), role_key))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"increment_role_count failed: {e}")
    finally:
        conn.close()


def get_role_counts(user_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT role_key, count FROM role_counts WHERE user_id = ? ORDER BY count DESC",
        (str(user_id),)
    ).fetchall()
    conn.close()
    return [(r["role_key"], r["count"]) for r in rows]


# ---- Badges ----

def unlock_badge(user_id, badge_key):
    conn = get_db()
    now = datetime.now(PARIS_TZ).isoformat()
    cur = conn.execute(
        "INSERT OR IGNORE INTO badges (user_id, badge_key, unlocked_at) VALUES (?, ?, ?)",
        (str(user_id), badge_key, now)
    )
    inserted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return inserted  # True si nouveau badge, False si déjà possédé


def get_user_badges(user_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT badge_key, unlocked_at FROM badges WHERE user_id = ? ORDER BY unlocked_at DESC",
        (str(user_id),)
    ).fetchall()
    conn.close()
    return [(r["badge_key"], r["unlocked_at"]) for r in rows]


# ---- Leaderboard ----

def get_leaderboard(metric="xp", limit=10):
    """metric: xp, games_won, correct_accusations, culprit_wins"""
    allowed = {"xp", "games_won", "correct_accusations", "culprit_wins", "games_played"}
    if metric not in allowed:
        metric = "xp"
    conn = get_db()
    rows = conn.execute(
        f"SELECT user_id, {metric} as value FROM player_stats "
        f"WHERE {metric} > 0 ORDER BY {metric} DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [(r["user_id"], r["value"]) for r in rows]


# ---- Game history ----

def save_game_history(game_id, guild_id, channel_id, host_id, scenario_key, size_mode,
                      player_count, culprit_id, culprit_survived, started_at, ended_at,
                      participants):
    conn = get_db()
    conn.execute("""INSERT OR REPLACE INTO game_history VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (game_id, str(guild_id), str(channel_id), str(host_id), scenario_key,
         size_mode, player_count, str(culprit_id) if culprit_id else None,
         1 if culprit_survived else 0, started_at, ended_at,
         json.dumps(participants)))
    conn.commit()
    conn.close()


def get_recent_games(guild_id, limit=10):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM game_history WHERE guild_id = ? ORDER BY ended_at DESC LIMIT ?",
        (str(guild_id), limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---- Cooldowns ----

def get_cooldown(user_id, key):
    conn = get_db()
    row = conn.execute(
        "SELECT until FROM cooldowns WHERE user_id = ? AND key = ?",
        (str(user_id), key)
    ).fetchone()
    conn.close()
    if not row:
        return None
    until = datetime.fromisoformat(row["until"])
    if until <= datetime.now(PARIS_TZ):
        return None
    return until


def set_cooldown(user_id, key, seconds):
    conn = get_db()
    until = (datetime.now(PARIS_TZ) + timedelta(seconds=seconds)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO cooldowns VALUES (?, ?, ?)",
        (str(user_id), key, until)
    )
    conn.commit()
    conn.close()


# ========================= HELPERS =========================

def embed_color():
    return 0x2b2d31


def success_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0x43b581)
    em.set_footer(text="Enquête ・ Meira")
    return em


def error_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0xf04747)
    em.set_footer(text="Enquête ・ Meira")
    return em


def info_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=embed_color())
    em.set_footer(text="Enquête ・ Meira")
    return em


def get_french_time():
    now = datetime.now(PARIS_TZ)
    JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
    return f"{JOURS_FR[now.weekday()]} {now.day} {MOIS_FR[now.month - 1]} {now.year} — {now.strftime('%Hh%M')}"


async def resolve_member(ctx, user_input):
    if not user_input:
        return None
    try:
        member_id = int(user_input.strip("<@!>"))
        m = ctx.guild.get_member(member_id)
        if m:
            return m
    except (ValueError, AttributeError):
        pass
    try:
        return await commands.MemberConverter().convert(ctx, user_input)
    except commands.CommandError:
        return None


async def resolve_user_or_id(ctx, user_input):
    """
    Retourne (display_obj, user_id) — marche même si l'user a leave.
    - (Member, id) s'il est dans la guild
    - (User, id) s'il n'est plus dans la guild mais existe sur Discord
    - (None, id) si ID valide mais compte supprimé
    - (None, None) si input vide/invalide
    """
    if not user_input:
        return None, None
    raw = user_input.strip()
    cleaned = raw.strip("<@!>")
    try:
        user_id = int(cleaned)
    except ValueError:
        try:
            m = await commands.MemberConverter().convert(ctx, raw)
            return m, m.id
        except commands.CommandError:
            pass
        try:
            u = await commands.UserConverter().convert(ctx, raw)
            return u, u.id
        except commands.CommandError:
            return None, None

    if ctx.guild:
        member = ctx.guild.get_member(user_id)
        if member:
            return member, user_id
    try:
        user = await bot.fetch_user(user_id)
        return user, user_id
    except discord.NotFound:
        return None, user_id
    except discord.HTTPException as e:
        log.warning(f"resolve_user_or_id: fetch_user({user_id}) a échoué : {e}")
        return None, user_id


def format_user_display(display_obj, user_id):
    if display_obj is not None:
        return f"{display_obj.mention} (`{display_obj.id}`)"
    return f"<@{user_id}> (`{user_id}`) *(hors serveur)*"


async def check_ban(ctx):
    if is_bot_banned(ctx.author.id):
        em = error_embed(
            "⛔ Accès refusé",
            "Tu as été **banni du bot Enquête**.\n"
            "Si tu penses que c'est une erreur, contacte un MJ ou un Sys."
        )
        await ctx.send(embed=em)
        return True
    return False


# ========================= XP / CLASSES =========================

def xp_for_level(level):
    """Progression exponentielle douce. Niveau 1 = 100 XP, 10 = 1585, 25 = 7880, 50 = 31623."""
    return int(100 * (level ** 1.5))


CLASS_TITLES = [
    # (niveau min, titre)
    (0,  "Civil"),
    (3,  "Novice"),
    (7,  "Apprenti détective"),
    (12, "Tanteishi"),          # Détective JP
    (18, "Inspecteur"),
    (25, "Commissaire"),
    (35, "Maître des indices"),
    (50, "Meitantei"),           # Grand détective JP
    (75, "Légende vivante"),
    (100, "Kami du mystère"),
]


def class_for_level(level):
    current = CLASS_TITLES[0][1]
    for min_lvl, title in CLASS_TITLES:
        if level >= min_lvl:
            current = title
        else:
            break
    return current


def next_class_info(level):
    """Retourne (prochain_titre, niveau_requis) ou (None, None) si déjà au max."""
    for min_lvl, title in CLASS_TITLES:
        if level < min_lvl:
            return title, min_lvl
    return None, None


async def award_xp(user_id, amount, ctx=None):
    """
    Ajoute de l'XP, gère le level up et les changements de classe.
    Retourne (new_level, leveled_up, new_class_unlocked_or_None).
    """
    async with stats_lock:
        stats = get_player_stats(user_id)
        new_xp = stats["xp"] + amount
        current_level = stats["level"]
        old_class = class_for_level(current_level)
        new_level = current_level
        while new_level < 100 and new_xp >= xp_for_level(new_level + 1):
            new_level += 1
        new_class = class_for_level(new_level)
        update_player_stats(user_id, xp=new_xp, level=new_level)

    leveled_up = new_level > current_level
    class_changed = new_class != old_class and leveled_up

    if leveled_up and ctx is not None:
        try:
            em = discord.Embed(
                title="🎉 Level up !",
                description=(
                    f"<@{user_id}> passe au niveau **{new_level}**\n"
                    + (f"\n🎭 Nouvelle classe : **{new_class}**" if class_changed else "")
                ),
                color=0xffd700,
            )
            em.set_footer(text="Enquête ・ Meira")
            await ctx.send(embed=em)
        except discord.HTTPException:
            pass

    return new_level, leveled_up, new_class if class_changed else None


# ========================= RÔLES =========================
# Chaque rôle a un nom japonais (flavor Meira), une description, un camp,
# et éventuellement une "action secrète" qui se déclenche entre les indices.
#
# Camps : "culprit" (coupable + complice), "innocent" (tout le reste)
# Actions : "inspect" (voir un rôle), "protect" (bloquer une action), "frame" (brouiller indice),
#           "reveal" (révéler un indice publiquement), "block" (empêcher une action), None

ROLES = {
    # ─── CAMP COUPABLE ─────────────────────────────────────────────────
    "culprit": {
        "name": "Yamikage",  # 闇影 — ombre de ténèbres
        "name_fr": "Coupable",
        "emoji": "🎭",
        "camp": "culprit",
        "description": (
            "Tu es le **Yamikage**, le véritable responsable du crime. "
            "Fais-toi passer pour un innocent, brouille les pistes, accuse les autres. "
            "Tu gagnes si tu survives au vote final."
        ),
        "action": "frame",
        "action_desc": "Brouille une piste : sélectionne un joueur, son nom sera associé à un indice mensonger.",
    },
    "accomplice": {
        "name": "Kagemusha",  # 影武者 — double d'ombre
        "name_fr": "Complice",
        "emoji": "🥷",
        "camp": "culprit",
        "description": (
            "Tu es le **Kagemusha**, complice du coupable. Tu connais son identité. "
            "Protège-le des soupçons, détourne l'attention. Tu gagnes si le coupable survit."
        ),
        "action": "alibi",
        "action_desc": "Fabrique un faux alibi pour un joueur (peut être toi, le coupable, ou quelqu'un d'autre).",
    },
    "assassin": {
        "name": "Shinobi",  # 忍 — assassin
        "name_fr": "Assassin",
        "emoji": "🗡️",
        "camp": "culprit",
        "description": (
            "Tu es le **Shinobi**, allié du coupable dans l'ombre. Tu peux éliminer un joueur "
            "entre deux indices (il ne pourra plus voter). Utilise ça avec stratégie."
        ),
        "action": "eliminate",
        "action_desc": "Élimine silencieusement un joueur : il perd son droit de vote pour la suite.",
    },

    # ─── CAMP INNOCENT — ENQUÊTEURS ─────────────────────────────────────
    "detective": {
        "name": "Tanteishi",  # 探偵師 — détective
        "name_fr": "Détective",
        "emoji": "🔍",
        "camp": "innocent",
        "description": (
            "Tu es le **Tanteishi**. Chaque phase d'action, tu peux inspecter un joueur "
            "et découvrir son camp (coupable ou innocent). Utilise cette info pour guider le groupe."
        ),
        "action": "inspect",
        "action_desc": "Inspecte un joueur : tu sauras s'il est du camp coupable ou innocent.",
    },
    "oracle": {
        "name": "Miko",  # 巫女 — prêtresse / oracle
        "name_fr": "Oracle",
        "emoji": "🔮",
        "camp": "innocent",
        "description": (
            "Tu es la **Miko**, médium du sanctuaire. Tu peux révéler le rôle exact d'un joueur "
            "une seule fois dans la partie. Utilise ton pouvoir au bon moment."
        ),
        "action": "reveal_role",
        "action_desc": "Révèle le rôle exact d'un joueur (une seule fois dans la partie).",
    },
    "witness": {
        "name": "Mokugekisha",  # 目撃者 — témoin oculaire
        "name_fr": "Témoin",
        "emoji": "👁️",
        "camp": "innocent",
        "description": (
            "Tu es le **Mokugekisha**. Tu as vu quelque chose : un **vrai indice supplémentaire** "
            "t'est envoyé en DM. À toi de décider si tu le partages et quand."
        ),
        "action": None,  # pas d'action active, juste un indice bonus en DM
    },
    "guardian": {
        "name": "Bantō",  # 番頭 — gardien / surveillant
        "name_fr": "Gardien",
        "emoji": "🛡️",
        "camp": "innocent",
        "description": (
            "Tu es le **Bantō**. Chaque phase, tu peux protéger un joueur : "
            "toute action secrète qui le cible sera annulée."
        ),
        "action": "protect",
        "action_desc": "Protège un joueur pour la phase : les actions qui le ciblent sont annulées.",
    },
    "journalist": {
        "name": "Shinbun-kisha",  # 新聞記者 — journaliste
        "name_fr": "Journaliste",
        "emoji": "📰",
        "camp": "innocent",
        "description": (
            "Tu es le **Shinbun-kisha**. Une fois par partie, tu peux publier un indice "
            "que tu as découvert, visible de tous. Attention à ce que tu publies."
        ),
        "action": "publish",
        "action_desc": "Publie un indice privé pour que tout le monde le voie (une seule fois).",
    },
    "doctor": {
        "name": "Iryōshi",  # 医療師 — médecin / guérisseur
        "name_fr": "Médecin",
        "emoji": "⚕️",
        "camp": "innocent",
        "description": (
            "Tu es l'**Iryōshi**. Tu peux sauver un joueur ciblé par l'Assassin cette phase. "
            "Si tu choisis correctement, il conserve son droit de vote."
        ),
        "action": "heal",
        "action_desc": "Soigne un joueur : s'il est ciblé par l'Assassin, il survit.",
    },
    "chief": {
        "name": "Shōnin",  # 商人 — chef / marchand influent
        "name_fr": "Notable",
        "emoji": "👑",
        "camp": "innocent",
        "description": (
            "Tu es le **Shōnin**, notable respecté. Ton vote compte **double** lors du vote final."
        ),
        "action": None,
    },
    "blocker": {
        "name": "Onmyōji",  # 陰陽師 — mage de l'équilibre
        "name_fr": "Onmyōji",
        "emoji": "☯️",
        "camp": "innocent",
        "description": (
            "Tu es l'**Onmyōji**. Tu peux choisir un joueur et bloquer son action secrète pour la phase."
        ),
        "action": "block",
        "action_desc": "Bloque l'action secrète d'un joueur pour cette phase.",
    },
    "vigilante": {
        "name": "Rōnin",  # 浪人 — samouraï errant
        "name_fr": "Justicier",
        "emoji": "⚔️",
        "camp": "innocent",
        "description": (
            "Tu es le **Rōnin**, justicier errant. Une seule fois dans la partie, tu peux éliminer "
            "quelqu'un. Attention : si tu élimines un innocent, tu perds la partie."
        ),
        "action": "vigilante_kill",
        "action_desc": "Élimine un joueur (une seule fois). Si c'est un innocent, tu perds.",
    },
    "civilian": {
        "name": "Yōgisha",  # 容疑者 — simple suspect
        "name_fr": "Civil",
        "emoji": "👤",
        "camp": "innocent",
        "description": (
            "Tu es un **Yōgisha**. Pas de pouvoir spécial — mais tu es innocent. "
            "Observe, déduis, participe au débat. Ta voix compte."
        ),
        "action": None,
    },
}


# ========================= COMPOSITION DES RÔLES =========================
# Selon le nombre de joueurs, on construit une composition équilibrée.
# Règle : environ 1/3 de coupables, 2/3 d'innocents, avec rôles spéciaux selon la taille.

def build_role_composition(player_count):
    """Retourne une liste de clés de rôles de taille exactement player_count."""
    if player_count < 3:
        raise ValueError("Il faut au moins 3 joueurs.")

    # Toujours 1 coupable
    roles = ["culprit"]

    # 3-5 joueurs : partie courte
    if player_count <= 5:
        if player_count >= 4:
            roles.append("detective")
        if player_count >= 5:
            roles.append("witness")
        while len(roles) < player_count:
            roles.append("civilian")
        return roles

    # 6-8 : ajoute complice + rôles de base
    if player_count <= 8:
        roles.extend(["accomplice", "detective", "witness", "doctor"])
        if player_count >= 7:
            roles.append("oracle")
        if player_count >= 8:
            roles.append("guardian")
        while len(roles) < player_count:
            roles.append("civilian")
        return roles[:player_count]

    # 9-12 : richesse
    if player_count <= 12:
        roles.extend(["accomplice", "detective", "witness", "doctor",
                      "oracle", "guardian", "journalist", "chief"])
        if player_count >= 11:
            roles.append("blocker")
        if player_count >= 12:
            roles.append("vigilante")
        while len(roles) < player_count:
            roles.append("civilian")
        return roles[:player_count]

    # 13-18 : introduit l'assassin (2e coupable actif)
    if player_count <= 18:
        roles.extend(["accomplice", "assassin", "detective", "witness", "doctor",
                      "oracle", "guardian", "journalist", "chief", "blocker",
                      "vigilante"])
        # Ajoute plus de détectives/gardiens pour équilibrer
        extras_pool = ["detective", "guardian", "witness", "doctor", "oracle"]
        i = 0
        while len(roles) < player_count - 2:
            roles.append(extras_pool[i % len(extras_pool)])
            i += 1
        while len(roles) < player_count:
            roles.append("civilian")
        return roles[:player_count]

    # 19+ : format massif, ajoute un 2e complice si > 22
    roles.extend(["accomplice", "assassin", "detective", "witness", "doctor",
                  "oracle", "guardian", "journalist", "chief", "blocker",
                  "vigilante"])
    if player_count >= 22:
        roles.append("accomplice")  # 2e complice
    if player_count >= 25:
        roles.append("detective")   # 2e détective
    extras_pool = ["detective", "guardian", "witness", "doctor", "oracle", "civilian"]
    i = 0
    while len(roles) < player_count - 3:
        roles.append(extras_pool[i % len(extras_pool)])
        i += 1
    while len(roles) < player_count:
        roles.append("civilian")
    return roles[:player_count]


# ========================= SCÉNARIOS =========================
# Ambiance Meira : japonaise subtile, pas de weeb lourd.
# Chaque scénario a un crime, des lieux, et des templates d'indices.
# Les indices utilisent les placeholders : {coupable}, {suspect1}, {suspect2}, {lieu}

SCENARIOS = [
    {
        "key": "ryokan",
        "title": "Meurtre au ryokan",
        "crime": "un client important a été retrouvé mort dans les bains d'un ryokan traditionnel",
        "places": ["les onsens extérieurs", "la salle de thé", "le jardin de pierre", "la chambre des hôtes", "le couloir des chambres"],
        "indices": [
            "Un témoin affirme avoir vu **{suspect2}** sortir précipitamment de {lieu} vers 22h30.",
            "Des traces humides menant à {lieu} correspondent aux chaussures de **{suspect1}**.",
            "La servante a surpris **{coupable}** en train de murmurer près de {lieu} peu avant le drame.",
        ],
    },
    {
        "key": "kabuki",
        "title": "Vol au théâtre kabuki",
        "crime": "le masque sacré du théâtre kabuki de Meira a disparu juste avant la représentation",
        "places": ["les coulisses", "la loge des acteurs", "la salle des costumes", "le parterre", "la régie"],
        "indices": [
            "Un régisseur a vu **{suspect1}** fouiller {lieu} une heure avant l'ouverture.",
            "Une empreinte de fard rouge retrouvée dans {lieu} correspond au maquillage de **{suspect2}**.",
            "Un accessoiriste jure avoir entendu **{coupable}** demander où était rangé le masque.",
        ],
    },
    {
        "key": "festival",
        "title": "Sabotage du festival de Hanami",
        "crime": "les lanternes du festival des cerisiers ont été sabotées avant la cérémonie d'ouverture",
        "places": ["le pont des cerisiers", "l'allée des lanternes", "la place centrale", "le stand de nourriture", "le pavillon du maire"],
        "indices": [
            "Des éclats de verre à {lieu} portent une trace qui correspond à **{suspect2}**.",
            "**{suspect1}** a été vu discutant longuement près de {lieu} juste avant l'incident.",
            "La caméra de surveillance a filmé une silhouette ressemblant à **{coupable}** quitter {lieu} rapidement.",
        ],
    },
    {
        "key": "port",
        "title": "Contrebande au port",
        "crime": "une cargaison de soie précieuse a été détournée pendant la nuit au port de Meira",
        "places": ["le quai nord", "le hangar principal", "le bureau des douanes", "le marché aux poissons", "la taverne du port"],
        "indices": [
            "Un marin raconte avoir vu **{suspect1}** charger une caisse suspecte à {lieu}.",
            "Une lettre adressée à **{coupable}** mentionnant la cargaison a été retrouvée dans {lieu}.",
            "**{suspect2}** était absent à son poste pendant les heures du vol, près de {lieu}.",
        ],
    },
    {
        "key": "dojo",
        "title": "Crime au dojo",
        "crime": "le maître du dojo Shirayuki a été retrouvé inconscient, son katana cérémoniel volé",
        "places": ["la salle d'entraînement", "l'autel du maître", "le jardin zen", "la cour intérieure", "les vestiaires"],
        "indices": [
            "Un disciple a vu **{suspect2}** sortir de {lieu} en tenue de combat, essoufflé.",
            "Des gouttes de sang dans {lieu} indiquent qu'une lutte brève a eu lieu près de **{suspect1}**.",
            "**{coupable}** avait récemment eu une dispute avec le maître au sujet de {lieu}.",
        ],
    },
    {
        "key": "tea",
        "title": "Empoisonnement au salon de thé",
        "crime": "un invité de marque a été empoisonné lors d'une cérémonie du thé privée",
        "places": ["la salle de cérémonie", "la cuisine du maître de thé", "le jardin d'accueil", "la salle d'attente", "le vestiaire"],
        "indices": [
            "Une petite fiole suspecte a été trouvée dans {lieu}, oubliée par **{suspect1}**.",
            "**{suspect2}** avait accès à la cuisine et a été vu entrer dans {lieu} juste avant la cérémonie.",
            "**{coupable}** connaissait parfaitement les herbes médicinales et a passé du temps à {lieu}.",
        ],
    },
    {
        "key": "sumo",
        "title": "Manipulation au tournoi de sumo",
        "crime": "un match du tournoi de sumo annuel a été truqué, le champion favori s'est effondré mystérieusement",
        "places": ["les vestiaires des lutteurs", "l'arène", "la salle d'échauffement", "le bureau des arbitres", "la salle des bains"],
        "indices": [
            "Un entraîneur a aperçu **{suspect2}** en grande discussion avec un parieur à {lieu}.",
            "**{suspect1}** avait des dettes de jeu et fréquentait souvent {lieu}.",
            "Une note anonyme signée d'initiales ressemblant à **{coupable}** a été retrouvée dans {lieu}.",
        ],
    },
    {
        "key": "imperial",
        "title": "Vol du trésor impérial",
        "crime": "une relique impériale de grande valeur a été dérobée du palais de Meira pendant la nuit",
        "places": ["la salle du trône", "la chambre du trésor", "le couloir des gardes", "le jardin intérieur", "la tour de l'horloge"],
        "indices": [
            "**{suspect1}** connaissait parfaitement les codes de sécurité de {lieu}.",
            "Un garde a vu **{suspect2}** rôder près de {lieu} à une heure interdite.",
            "Une clé maîtresse a été copiée récemment, et **{coupable}** travaillait près de {lieu}.",
        ],
    },
    {
        "key": "school",
        "title": "Scandale à l'école",
        "crime": "les sujets d'examen de la prestigieuse Académie de Meira ont fuité avant l'épreuve",
        "places": ["le bureau du directeur", "la salle des professeurs", "la bibliothèque", "l'imprimerie", "le foyer des élèves"],
        "indices": [
            "**{suspect1}** a été vu en train d'emprunter la clé de {lieu} sans autorisation.",
            "Des photocopies récentes dans {lieu} correspondent aux sujets volés — **{suspect2}** en était responsable.",
            "**{coupable}** avait accès aux archives de {lieu} et a mystérieusement disparu ce matin-là.",
        ],
    },
    {
        "key": "geisha",
        "title": "Disparition d'une geisha",
        "crime": "une célèbre geisha du quartier Hanamachi a disparu après sa dernière représentation",
        "places": ["le salon principal", "la loge privée", "le jardin arrière", "le pont aux lanternes", "la maison de thé voisine"],
        "indices": [
            "Un client affirme avoir vu **{suspect2}** attendre à {lieu} bien après la fin du spectacle.",
            "Le kimono déchiré retrouvé à {lieu} porte des traces qui désignent **{suspect1}**.",
            "**{coupable}** avait été éconduit par la geisha la veille, près de {lieu}.",
        ],
    },
    # Scénarios plus universels (moins focus Japon)
    {
        "key": "heist",
        "title": "Braquage du casino",
        "crime": "le coffre-fort du casino Meira a été vidé pendant une soirée VIP",
        "places": ["la salle des coffres", "le salon VIP", "la régie de surveillance", "les cuisines", "le parking souterrain"],
        "indices": [
            "**{suspect1}** a désactivé son badge à un moment précis et se trouvait près de {lieu}.",
            "Une carte magnétique clonée a été retrouvée dans {lieu}, liée à **{suspect2}**.",
            "**{coupable}** avait accès aux plans du bâtiment et a consulté {lieu} récemment.",
        ],
    },
    {
        "key": "mansion",
        "title": "Meurtre au manoir",
        "crime": "le riche propriétaire du manoir Tsukiyama a été retrouvé sans vie dans son bureau",
        "places": ["la bibliothèque", "le bureau privé", "la salle à manger", "le fumoir", "la cave à vin"],
        "indices": [
            "**{suspect1}** a été entendu se disputer avec la victime dans {lieu}.",
            "Une empreinte digitale de **{suspect2}** a été relevée sur la poignée de {lieu}.",
            "**{coupable}** avait menacé la victime lors du dernier dîner qui a eu lieu à {lieu}.",
        ],
    },
    {
        "key": "hospital",
        "title": "Disparition à l'hôpital",
        "crime": "un patient placé sous haute surveillance a disparu mystérieusement de sa chambre",
        "places": ["le service de garde", "la salle des médicaments", "les couloirs du 3e", "l'entrée des urgences", "la morgue"],
        "indices": [
            "**{suspect1}** était de garde cette nuit-là et a été absent de {lieu} pendant 20 minutes.",
            "Une seringue vide retrouvée dans {lieu} portait les empreintes de **{suspect2}**.",
            "**{coupable}** avait demandé plusieurs fois à être transféré à {lieu}.",
        ],
    },
    {
        "key": "academy",
        "title": "Sabotage au laboratoire",
        "crime": "une expérience scientifique cruciale de l'Académie a été sabotée la nuit précédant sa présentation",
        "places": ["le laboratoire principal", "la salle serveur", "l'amphithéâtre", "le local technique", "la salle de conférence"],
        "indices": [
            "**{suspect1}** avait un badge d'accès à {lieu} et était présent après les heures.",
            "Les caméras de {lieu} montrent **{suspect2}** en train de manipuler l'équipement.",
            "**{coupable}** était en rivalité directe avec le responsable du projet au {lieu}.",
        ],
    },
]


# ========================= PRESETS DE TAILLE =========================
# Choix proposés au lancement d'une enquête.

SIZE_PRESETS = {
    "small": {
        "emoji": "🍃",
        "label": "Petite partie",
        "description": "3 à 6 joueurs — Partie rapide, rôles de base",
        "min": 3, "max": 6, "default": 5,
    },
    "medium": {
        "emoji": "🏮",
        "label": "Partie moyenne",
        "description": "6 à 12 joueurs — Équilibrée, rôles variés",
        "min": 6, "max": 12, "default": 9,
    },
    "large": {
        "emoji": "🎎",
        "label": "Grande partie",
        "description": "12 à 20 joueurs — Tous les rôles + Assassin",
        "min": 12, "max": 20, "default": 15,
    },
    "massive": {
        "emoji": "🏯",
        "label": "Partie massive",
        "description": "20 à 40 joueurs — Format event, duplication de rôles",
        "min": 20, "max": 40, "default": 25,
    },
    "custom": {
        "emoji": "✏️",
        "label": "Personnalisée",
        "description": "Tu choisis le nombre exact de participants (3 à 40)",
        "min": 3, "max": 40, "default": 8,
    },
}


# ========================= BADGES =========================
# Clé : description unlock conditions, déclenchés en fin de partie

BADGES = {
    # Badges de progression
    "first_game":      {"emoji": "🎲", "name": "Première affaire",          "desc": "Participer à sa première enquête"},
    "played_10":       {"emoji": "📋", "name": "Habitué",                   "desc": "10 enquêtes jouées"},
    "played_50":       {"emoji": "📚", "name": "Enquêteur aguerri",         "desc": "50 enquêtes jouées"},
    "played_100":      {"emoji": "🏯", "name": "Vétéran de Meira",          "desc": "100 enquêtes jouées"},

    # Badges de victoire
    "first_win":       {"emoji": "🏆", "name": "Première victoire",         "desc": "Gagner sa première partie"},
    "won_10":          {"emoji": "🥇", "name": "Triomphe",                  "desc": "10 parties gagnées"},
    "won_25":          {"emoji": "💎", "name": "Série d'or",                "desc": "25 parties gagnées"},

    # Badges détective
    "first_catch":     {"emoji": "🎯", "name": "Premier coup",              "desc": "Trouver le coupable pour la première fois"},
    "sharp_eye":       {"emoji": "🔍", "name": "Œil affûté",                "desc": "Trouver le coupable 10 fois"},
    "sherlock":        {"emoji": "🕵️", "name": "Sherlock de Meira",         "desc": "Trouver le coupable 25 fois"},

    # Badges coupable
    "first_escape":    {"emoji": "🎭", "name": "Parfait menteur",            "desc": "Survivre pour la première fois en tant que coupable"},
    "master_liar":     {"emoji": "👹", "name": "Maître de l'ombre",          "desc": "Survivre 10 fois en tant que coupable"},
    "yamikage_god":    {"emoji": "🌑", "name": "Yamikage immortel",          "desc": "Survivre 25 fois en tant que coupable"},

    # Badges spéciaux
    "flawless":        {"emoji": "✨", "name": "Sans fausse note",           "desc": "Gagner 3 parties d'affilée"},
    "comeback":        {"emoji": "🔥", "name": "Retour au sommet",           "desc": "Gagner après 5 défaites consécutives"},
    "never_wrong":     {"emoji": "🧠", "name": "Jamais tort",                "desc": "5 bonnes accusations sans une seule fausse"},
    "social":          {"emoji": "🗣️", "name": "Avocat du diable",          "desc": "Participer à 20 débats publics"},
    "role_collector":  {"emoji": "🎲", "name": "Collectionneur",             "desc": "Jouer au moins 5 rôles différents"},
    "meitantei":       {"emoji": "🎩", "name": "Meitantei",                  "desc": "Atteindre le niveau 50"},
    "kami":            {"emoji": "⛩️", "name": "Kami du mystère",            "desc": "Atteindre le niveau 100"},
}


def check_and_award_badges(user_id):
    """
    Vérifie toutes les conditions de badges pour un user et débloque ceux qu'il mérite.
    Retourne la liste des clés de badges nouvellement débloqués.
    """
    stats = get_player_stats(user_id)
    role_counts = dict(get_role_counts(user_id))
    owned = {b for b, _ in get_user_badges(user_id)}
    newly_unlocked = []

    def try_unlock(key, condition):
        if condition and key not in owned and unlock_badge(user_id, key):
            newly_unlocked.append(key)

    try_unlock("first_game",    stats["games_played"] >= 1)
    try_unlock("played_10",     stats["games_played"] >= 10)
    try_unlock("played_50",     stats["games_played"] >= 50)
    try_unlock("played_100",    stats["games_played"] >= 100)

    try_unlock("first_win",     stats["games_won"] >= 1)
    try_unlock("won_10",        stats["games_won"] >= 10)
    try_unlock("won_25",        stats["games_won"] >= 25)

    try_unlock("first_catch",   stats["correct_accusations"] >= 1)
    try_unlock("sharp_eye",     stats["correct_accusations"] >= 10)
    try_unlock("sherlock",      stats["correct_accusations"] >= 25)

    try_unlock("first_escape",  stats["culprit_wins"] >= 1)
    try_unlock("master_liar",   stats["culprit_wins"] >= 10)
    try_unlock("yamikage_god",  stats["culprit_wins"] >= 25)

    # Never wrong : au moins 5 correct et aucune erreur
    try_unlock("never_wrong",   stats["correct_accusations"] >= 5 and stats["wrong_accusations"] == 0)

    # Role collector : au moins 5 rôles différents joués
    try_unlock("role_collector", len(role_counts) >= 5)

    # Meitantei / Kami (niveaux)
    try_unlock("meitantei", stats["level"] >= 50)
    try_unlock("kami",      stats["level"] >= 100)

    # Note : flawless (3 wins d'affilée) et comeback (gagner après 5 défaites) nécessitent un tracking
    # supplémentaire qu'on pourra ajouter plus tard via un champ "streak".

    return newly_unlocked


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()


def get_prefix(bot, message):
    return get_prefix_cached()


bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


# ========================= GLOBAL CHANNEL CHECK =========================

class ChannelNotAllowedError(commands.CheckFailure):
    pass


@bot.check
async def check_allowed_channel(ctx):
    """Sys+ bypass, les autres doivent être dans un salon autorisé."""
    if has_min_rank(ctx.author.id, 3):
        return True
    if ctx.guild is None:
        return True
    if is_channel_allowed(ctx.guild.id, ctx.channel.id):
        return True
    raise ChannelNotAllowedError("Salon non autorisé.")


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    log.info(f"Enquête connecté : {bot.user} ({bot.user.id})")
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.playing, name="une enquête à Meira")
    )


@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if message.guild:
        track_message(message.guild.id, message.author.id)
    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandInvokeError):
        error = error.original
    if isinstance(error, ChannelNotAllowedError):
        try:
            await ctx.message.add_reaction("❌")
        except discord.HTTPException:
            pass
        return
    if isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Impossible de trouver cet utilisateur."))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=error_embed(
            "❌ Argument manquant",
            f"Il te manque l'argument : `{error.param.name}`."
        ))
    elif isinstance(error, commands.BadArgument):
        await ctx.send(embed=error_embed("❌ Argument invalide", str(error)))
    elif isinstance(error, commands.CommandOnCooldown):
        await ctx.send(embed=error_embed(
            "⏰ Cooldown",
            f"Reviens dans {int(error.retry_after)}s."
        ))
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        log.error(
            f"Erreur non gérée '{ctx.command}' par {ctx.author} : {error}\n"
            + "".join(traceback.format_exception(type(error), error, error.__traceback__))
        )
        try:
            await ctx.send(embed=error_embed(
                "❌ Erreur interne",
                "Une erreur inattendue est survenue. Les logs ont été générés."
            ))
        except discord.HTTPException:
            pass


# ========================= LOG =========================

async def send_log(guild, action, author, target=None, desc=None, color=0x2b2d31):
    channel_id = get_log_channel(guild.id)
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    em = discord.Embed(title=f"📋 {action}", color=color)
    em.add_field(name="Auteur", value=f"{author.mention} (`{author.id}`)", inline=True)
    if target:
        em.add_field(name="Cible", value=f"{target.mention} (`{target.id}`)", inline=True)
    if desc:
        em.add_field(name="Détail", value=desc, inline=False)
    em.set_footer(text=get_french_time())
    try:
        await channel.send(embed=em)
    except discord.HTTPException as e:
        log.warning(f"send_log: impossible d'envoyer dans {channel.id} : {e}")


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                      PARTIE 2 — CŒUR DU JEU                              ║
# ║  Machine à états : SETUP → RECRUITING → ROLES → INDICES/ACTIONS →        ║
# ║                    DEBATE → VOTE → RESOLUTION                            ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# Une partie active par salon (clé : channel_id). Empêche de lancer 2 parties
# dans le même salon. Global en mémoire.
active_games = {}


# ========================= GAME CLASS =========================

class Game:
    """
    État complet d'une partie. Toutes les manipulations passent par le GameManager
    pour éviter les états incohérents.
    """
    def __init__(self, ctx, size_mode, player_count, scenario_key=None):
        self.game_id = f"{ctx.guild.id}-{ctx.channel.id}-{int(datetime.now(PARIS_TZ).timestamp())}"
        self.ctx = ctx
        self.guild = ctx.guild
        self.channel = ctx.channel
        self.host = ctx.author
        self.size_mode = size_mode
        self.target_player_count = player_count

        # Scénario choisi (random si None)
        if scenario_key:
            self.scenario = next((s for s in SCENARIOS if s["key"] == scenario_key), random.choice(SCENARIOS))
        else:
            self.scenario = random.choice(SCENARIOS)

        # État de la partie
        self.phase = "SETUP"          # SETUP → RECRUITING → ROLES → INDICES → DEBATE → VOTE → RESOLUTION → ENDED
        self.participants = []         # liste de discord.Member (inscrits)
        self.roles_assignment = {}     # user_id → role_key
        self.culprit_id = None
        self.accomplice_ids = set()
        self.eliminated_ids = set()    # Joueurs tués par le Shinobi (peuvent plus voter)
        self.vigilante_used = False    # Le Rōnin n'a qu'un usage
        self.oracle_used = False       # La Miko n'a qu'un usage
        self.journalist_used = False   # Le Journaliste n'a qu'un usage

        # Données narratives
        self.lieu = random.choice(self.scenario["places"])
        self.indices_revealed = []     # indices déjà révélés au public
        self.false_clues = []          # indices brouillés par le Yamikage (pour l'affichage final)

        # Actions secrètes de la phase courante
        # {user_id: {"action": "inspect", "target": user_id, "data": ...}}
        self.current_actions = {}
        self.protected_ids = set()     # protégés par Bantō cette phase
        self.blocked_ids = set()       # bloqués par Onmyōji cette phase

        # Messages Discord clés (pour édition)
        self.recruiting_message = None
        self.debate_end_time = None

        # Votes finaux
        self.votes = {}                # voter_id → target_user_id
        self.final_accused = None      # user_id de la personne éliminée par vote

        # Timers
        self.started_at = datetime.now(PARIS_TZ)
        self.ended_at = None

    # ---- Helpers internes ----

    def get_member(self, user_id):
        """Retourne le Member Discord correspondant à un user_id de la partie."""
        for m in self.participants:
            if m.id == user_id:
                return m
        return None

    def get_role(self, user_id):
        key = self.roles_assignment.get(user_id)
        if not key:
            return None
        return ROLES[key]

    def alive_players(self):
        return [m for m in self.participants if m.id not in self.eliminated_ids]

    def players_by_camp(self, camp):
        return [m for m in self.participants
                if self.roles_assignment.get(m.id) and ROLES[self.roles_assignment[m.id]]["camp"] == camp]


# ========================= GAMEMANAGER =========================

class GameManager:
    """Orchestre les phases d'une partie."""

    @staticmethod
    async def start_game(ctx, size_mode, player_count, scenario_key=None):
        """Point d'entrée. Crée la partie et lance le recrutement."""
        if ctx.channel.id in active_games:
            return await ctx.send(embed=error_embed(
                "❌ Partie en cours",
                "Une enquête est déjà active dans ce salon. Attends qu'elle se termine."
            ))

        game = Game(ctx, size_mode, player_count, scenario_key)
        active_games[ctx.channel.id] = game
        try:
            await GameManager.run_recruiting(game)
            if game.phase != "ENDED":  # Si pas annulée au recrutement
                await GameManager.run_roles_assignment(game)
                await GameManager.run_main_loop(game)
                await GameManager.run_resolution(game)
        except Exception as e:
            log.error(f"Erreur dans la partie {game.game_id} : {e}\n{traceback.format_exc()}")
            try:
                await ctx.send(embed=error_embed(
                    "❌ Partie interrompue",
                    "Une erreur est survenue. La partie est annulée."
                ))
            except discord.HTTPException:
                pass
        finally:
            active_games.pop(ctx.channel.id, None)

    # ─────────────────── PHASE 1 : RECRUTEMENT ────────────────────

    @staticmethod
    async def run_recruiting(game: Game):
        """Ouvre les inscriptions avec boutons Rejoindre/Partir/Lancer/Annuler."""
        game.phase = "RECRUITING"
        view = RecruitingView(game, timeout=600)  # 10 min max

        em = GameManager.build_recruiting_embed(game)
        msg = await game.channel.send(embed=em, view=view)
        game.recruiting_message = msg
        view.message = msg

        # Le host participe par défaut
        game.participants.append(game.host)

        # Refresh régulier de l'embed pour afficher les nouveaux participants
        await view.wait()

        # À la sortie du wait, soit le host a lancé la partie, soit elle a été annulée, soit timeout
        if view.cancelled or len(game.participants) < 3:
            game.phase = "ENDED"
            try:
                await msg.edit(
                    embed=error_embed(
                        "❌ Enquête annulée",
                        "Pas assez de participants ou partie annulée par le host."
                    ),
                    view=None,
                )
            except discord.HTTPException:
                pass
            return

        # Ajuste la cible réelle selon le nombre d'inscrits
        game.target_player_count = len(game.participants)

    @staticmethod
    def build_recruiting_embed(game: Game):
        participants_list = "\n".join(
            f"• {m.mention}" + (" *(host)*" if m.id == game.host.id else "")
            for m in game.participants
        ) if game.participants else "*Aucun participant pour l'instant*"

        size_preset = SIZE_PRESETS.get(game.size_mode, SIZE_PRESETS["custom"])
        em = discord.Embed(
            title=f"🕵️ {game.scenario['title']}",
            description=(
                f"**{game.scenario['crime'].capitalize()}**\n\n"
                f"📋 Mode : {size_preset['emoji']} **{size_preset['label']}**\n"
                f"👥 Objectif : **{game.target_player_count}** joueurs  ・  "
                f"Inscrits : **{len(game.participants)}**\n"
                f"👑 Hôte : {game.host.mention}\n\n"
                f"**Participants :**\n{participants_list}\n\n"
                f"Clique sur **Rejoindre** pour participer. Le host peut lancer dès qu'il y a assez de monde."
            ),
            color=0x3498db,
        )
        em.set_footer(text="Enquête ・ Meira")
        return em

    # ─────────────────── PHASE 2 : ATTRIBUTION DES RÔLES ────────────────────

    @staticmethod
    async def run_roles_assignment(game: Game):
        """Compose la liste de rôles, mélange, attribue, envoie en DM."""
        game.phase = "ROLES"
        n = len(game.participants)
        role_keys = build_role_composition(n)
        random.shuffle(role_keys)

        # Shuffle des joueurs aussi pour plus d'équité
        players_shuffled = list(game.participants)
        random.shuffle(players_shuffled)

        for member, role_key in zip(players_shuffled, role_keys):
            game.roles_assignment[member.id] = role_key
            role = ROLES[role_key]
            if role_key == "culprit":
                game.culprit_id = member.id
            elif role_key == "accomplice":
                game.accomplice_ids.add(member.id)

            # DM du rôle
            await GameManager.dm_role(game, member, role_key)

        # Stats : incrémente le compteur de rôle joué
        for uid, rkey in game.roles_assignment.items():
            increment_role_count(uid, rkey)

        # Annonce publique
        em = discord.Embed(
            title="🎭 Les rôles ont été distribués",
            description=(
                f"**{n}** joueurs reçoivent leur rôle secret en message privé.\n\n"
                f"📍 **Lieu du crime :** {game.lieu}\n\n"
                f"*Ceux dont les DMs sont fermés ne recevront rien : qu'ils disent MJ.*\n"
                f"*L'enquête commence dans quelques secondes...*"
            ),
            color=0x9b59b6,
        )
        em.set_footer(text="Enquête ・ Meira")
        await game.channel.send(embed=em)
        await asyncio.sleep(8)

    @staticmethod
    async def dm_role(game: Game, member: discord.Member, role_key: str):
        """Envoie un DM avec le rôle + info spéciale selon le rôle."""
        role = ROLES[role_key]
        description = (
            f"🎭 Ton rôle : **{role['name']}** ({role['name_fr']})\n"
            f"🏮 Camp : **{'Coupable' if role['camp'] == 'culprit' else 'Innocent'}**\n\n"
            f"{role['description']}"
        )

        # Info spéciale pour le complice : connaît le coupable
        if role_key == "accomplice":
            culprit_member = game.get_member(game.culprit_id)
            if culprit_member:
                description += f"\n\n🎭 **Le coupable est : {culprit_member.display_name}**"

        # Info spéciale pour le coupable : connaît ses complices
        if role_key == "culprit":
            accomplices = [game.get_member(uid) for uid in game.accomplice_ids]
            accomplices = [a for a in accomplices if a is not None]
            # Détecte aussi l'assassin comme "allié" du culprit
            assassins = [m for m in game.participants
                         if game.roles_assignment.get(m.id) == "assassin"]
            allies = accomplices + assassins
            if allies:
                description += "\n\n🤝 **Tes alliés :**\n"
                for a in allies:
                    r = ROLES[game.roles_assignment[a.id]]
                    description += f"• {a.display_name} — *{r['name_fr']}*\n"

        # Info spéciale pour l'assassin
        if role_key == "assassin":
            culprit_member = game.get_member(game.culprit_id)
            if culprit_member:
                description += f"\n\n🎭 **Le coupable est : {culprit_member.display_name}**"

        # Indice bonus pour le témoin
        if role_key == "witness":
            culprit_member = game.get_member(game.culprit_id)
            if culprit_member:
                description += (
                    f"\n\n👁️ **Ton indice privé :**\n"
                    f"*Tu as aperçu **{culprit_member.display_name}** quitter {game.lieu} "
                    f"avec un air coupable. Tu en es sûr à 100%.*"
                )

        em = discord.Embed(
            title=f"🕵️ {game.scenario['title']}",
            description=description,
            color=0xe74c3c if role['camp'] == 'culprit' else 0x3498db,
        )
        em.set_footer(text=f"Enquête ・ Partie #{game.game_id[-8:]}")

        try:
            await member.send(embed=em)
        except discord.Forbidden:
            log.info(f"DM fermé pour {member} ({member.id}), rôle {role_key}")
            # Prévient dans le salon public sans révéler le rôle
            try:
                await game.channel.send(
                    embed=error_embed(
                        "⚠️ DM fermé",
                        f"{member.mention}, tes DMs sont fermés. Demande à un MJ de te donner ton rôle en privé."
                    )
                )
            except discord.HTTPException:
                pass
        except discord.HTTPException as e:
            log.warning(f"Échec DM rôle à {member} : {e}")

    # ─────────────────── PHASE 3 : BOUCLE INDICES + ACTIONS ────────────────────

    @staticmethod
    async def run_main_loop(game: Game):
        """
        Alterne entre révélation d'un indice et phase d'actions secrètes.
        3 indices, donc 3 cycles. Après chaque indice, fenêtre de 45s pour les actions en DM.
        """
        game.phase = "INDICES"
        culprit_name = game.get_member(game.culprit_id).display_name
        all_alive = game.alive_players()
        others = [m for m in all_alive if m.id != game.culprit_id]
        random.shuffle(others)
        suspect1 = others[0].display_name if len(others) >= 1 else "Inconnu"
        suspect2 = others[1].display_name if len(others) >= 2 else "Inconnu"

        templates = list(game.scenario["indices"])
        random.shuffle(templates)

        for i, template in enumerate(templates[:3]):
            # 1. Résout les actions secrètes de la PHASE PRÉCÉDENTE (sauf pour la toute première)
            # (La 1re phase d'action a lieu APRÈS le 1er indice)

            # 2. Révèle l'indice (avec brouillage éventuel du Yamikage)
            indice_text = template.format(
                coupable=culprit_name,
                suspect1=suspect1,
                suspect2=suspect2,
                lieu=game.lieu,
            )

            # Le Yamikage peut avoir brouillé un indice à la phase précédente
            # On remplace le nom du coupable par celui de la cible du frame
            if game.false_clues:
                frame = game.false_clues.pop(0)  # consomme le prochain faux indice
                if frame["target_id"] != game.culprit_id:
                    target_member = game.get_member(frame["target_id"])
                    if target_member:
                        indice_text = indice_text.replace(culprit_name, target_member.display_name, 1)
                        indice_text += "\n\n*(un détail trouble t'empêche d'être 100% sûr de cet indice)*"

            game.indices_revealed.append(indice_text)

            em = discord.Embed(
                title=f"🔍 Indice {i+1} / 3",
                description=indice_text,
                color=0x3498db,
            )
            em.set_footer(text="Enquête ・ Meira")
            await game.channel.send(embed=em)

            # 3. Phase d'actions secrètes en DM (sauf après le dernier indice)
            if i < 2:
                await GameManager.run_action_phase(game, phase_num=i+1)

        # Après le 3e indice, on passe directement au débat
        await GameManager.run_debate(game)

    @staticmethod
    async def run_action_phase(game: Game, phase_num: int):
        """
        Envoie à chaque joueur ayant un rôle actif un DM avec son bouton d'action.
        Attend 45s puis résout toutes les actions.
        """
        game.current_actions = {}
        game.protected_ids = set()
        game.blocked_ids = set()

        # Annonce publique
        em = discord.Embed(
            title=f"🌙 Phase d'action — {phase_num}/2",
            description=(
                "Ceux qui ont un pouvoir ont reçu un DM pour l'utiliser.\n"
                "⏳ Ils ont **45 secondes** avant la révélation du prochain indice."
            ),
            color=0x9b59b6,
        )
        em.set_footer(text="Enquête ・ Meira")
        await game.channel.send(embed=em)

        # Envoi des DM d'action
        dm_tasks = []
        for member in game.alive_players():
            role_key = game.roles_assignment.get(member.id)
            if not role_key:
                continue
            role = ROLES[role_key]
            if role.get("action") is None:
                continue  # Rôle passif (civil, chef, témoin...)

            # Vérifie les conditions d'usage unique
            if role_key == "oracle" and game.oracle_used:
                continue
            if role_key == "journalist" and game.journalist_used:
                continue
            if role_key == "vigilante" and game.vigilante_used:
                continue

            dm_tasks.append(GameManager.send_action_dm(game, member, role_key))

        await asyncio.gather(*dm_tasks, return_exceptions=True)

        # Attente 45s pour que les joueurs jouent
        await asyncio.sleep(45)

        # Résolution des actions
        await GameManager.resolve_actions(game)

    @staticmethod
    async def send_action_dm(game: Game, member: discord.Member, role_key: str):
        """Envoie un DM avec le Select d'action du joueur."""
        role = ROLES[role_key]
        others = [m for m in game.alive_players() if m.id != member.id]
        if not others:
            return

        try:
            view = ActionView(game, member, role_key, others)
            em = discord.Embed(
                title=f"🎭 {role['name']} — Action secrète",
                description=(
                    f"{role['action_desc']}\n\n"
                    f"⏳ Tu as **45 secondes** pour choisir ta cible.\n"
                    f"*Sans réponse, ton action sera perdue pour cette phase.*"
                ),
                color=0x9b59b6,
            )
            em.set_footer(text=f"Enquête ・ Partie #{game.game_id[-8:]}")
            await member.send(embed=em, view=view)
        except discord.Forbidden:
            log.info(f"Action DM fermé pour {member} ({role_key})")
        except discord.HTTPException as e:
            log.warning(f"Échec DM action à {member} : {e}")

    @staticmethod
    async def resolve_actions(game: Game):
        """
        Résout toutes les actions collectées pendant la phase.
        Ordre : blocks → protects → frames/alibi → inspects → reveals → heals → eliminations.
        """
        # 1. Blocks (Onmyōji) s'appliquent d'abord
        for uid, act in list(game.current_actions.items()):
            if act["action"] == "block":
                game.blocked_ids.add(act["target"])

        # 2. Protects (Bantō)
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "protect":
                game.protected_ids.add(act["target"])

        # 3. Frames (Yamikage) → ajoute un faux indice
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "frame":
                game.false_clues.append({"target_id": act["target"]})

        # 4. Alibi (Kagemusha) → envoie un message public rassurant sur la cible
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "alibi":
                target = game.get_member(act["target"])
                if target:
                    try:
                        em = discord.Embed(
                            title="💬 Alibi",
                            description=(
                                f"**Un témoin vient de confirmer :** {target.mention} "
                                f"se trouvait ailleurs au moment du crime.\n"
                                f"*(certains doutent de la fiabilité de ce témoignage)*"
                            ),
                            color=0x95a5a6,
                        )
                        em.set_footer(text="Enquête ・ Meira")
                        await game.channel.send(embed=em)
                    except discord.HTTPException:
                        pass

        # 5. Inspect (Tanteishi) → DM le résultat
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "inspect":
                target = game.get_member(act["target"])
                if target:
                    target_role = ROLES[game.roles_assignment[target.id]]
                    camp_display = "🎭 Coupable" if target_role["camp"] == "culprit" else "👤 Innocent"
                    inspector = game.get_member(uid)
                    if inspector:
                        try:
                            em = discord.Embed(
                                title="🔍 Résultat de ton inspection",
                                description=f"**{target.display_name}** appartient au camp : {camp_display}",
                                color=0x3498db,
                            )
                            em.set_footer(text="Enquête ・ Meira")
                            await inspector.send(embed=em)
                        except discord.HTTPException:
                            pass

        # 6. Reveal role (Miko) → révèle publiquement le rôle EXACT d'une cible
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "reveal_role":
                if game.oracle_used:
                    continue
                game.oracle_used = True
                target = game.get_member(act["target"])
                if target:
                    target_role = ROLES[game.roles_assignment[target.id]]
                    try:
                        em = discord.Embed(
                            title="🔮 Révélation de la Miko",
                            description=(
                                f"La prêtresse invoque les esprits...\n\n"
                                f"**{target.display_name}** est le/la **{target_role['name_fr']}** "
                                f"({target_role['emoji']})"
                            ),
                            color=0xf1c40f,
                        )
                        em.set_footer(text="Enquête ・ Meira")
                        await game.channel.send(embed=em)
                    except discord.HTTPException:
                        pass

        # 7. Publish (Journaliste) → publie le dernier indice perso
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "publish":
                if game.journalist_used:
                    continue
                game.journalist_used = True
                journalist = game.get_member(uid)
                if journalist and game.indices_revealed:
                    try:
                        em = discord.Embed(
                            title="📰 Édition spéciale",
                            description=(
                                f"**Le Shinbun-kisha publie un article :**\n\n"
                                f"*\"J'ai enquêté personnellement et je confirme ce détail :\"*\n\n"
                                f"> {game.indices_revealed[-1]}"
                            ),
                            color=0xe67e22,
                        )
                        em.set_footer(text="Enquête ・ Meira")
                        await game.channel.send(embed=em)
                    except discord.HTTPException:
                        pass

        # 8. Eliminate (Shinobi) → élimine une cible sauf si protégée ou soignée
        # D'abord on collecte, puis on applique en tenant compte des heals
        eliminations_attempts = []
        heal_targets = set()
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "eliminate":
                eliminations_attempts.append((uid, act["target"]))
            if act["action"] == "heal":
                heal_targets.add(act["target"])

        for killer_id, target_id in eliminations_attempts:
            if target_id in game.protected_ids:
                continue  # Bantō a protégé
            if target_id in heal_targets:
                # Iryōshi a sauvé
                target = game.get_member(target_id)
                if target:
                    try:
                        em = discord.Embed(
                            title="⚕️ Guérison miraculeuse",
                            description=(
                                f"**{target.display_name}** a failli être éliminé cette phase "
                                f"mais a été sauvé in extremis."
                            ),
                            color=0x2ecc71,
                        )
                        em.set_footer(text="Enquête ・ Meira")
                        await game.channel.send(embed=em)
                    except discord.HTTPException:
                        pass
                continue
            game.eliminated_ids.add(target_id)
            target = game.get_member(target_id)
            if target:
                try:
                    em = discord.Embed(
                        title="🗡️ Un joueur a été éliminé",
                        description=(
                            f"**{target.display_name}** a été trouvé inconscient. "
                            f"Il ne pourra pas voter lors du vote final."
                        ),
                        color=0xe74c3c,
                    )
                    em.set_footer(text="Enquête ・ Meira")
                    await game.channel.send(embed=em)
                except discord.HTTPException:
                    pass

        # 9. Vigilante (Rōnin) → élimine mais si innocent, il perd la partie
        for uid, act in list(game.current_actions.items()):
            if uid in game.blocked_ids:
                continue
            if act["action"] == "vigilante_kill":
                if game.vigilante_used:
                    continue
                game.vigilante_used = True
                target_id = act["target"]
                target = game.get_member(target_id)
                if not target:
                    continue
                target_role = ROLES[game.roles_assignment[target_id]]
                game.eliminated_ids.add(target_id)
                # Si le Rōnin a tué un innocent, on le marque pour qu'il perde à la fin
                if target_role["camp"] == "innocent":
                    # On stocke le fait qu'il ait raté (sera pénalisé en résolution)
                    game.roles_assignment[uid] = "civilian"  # rôle dégradé (il ne compte plus comme innocent loyal)
                    # Note : pour simplifier, on le laisse dans le camp innocent mais on lui donne pas de win.
                    # On utilise un marker à part.
                    if not hasattr(game, "vigilante_failed"):
                        game.vigilante_failed = set()
                    game.vigilante_failed = {uid}
                try:
                    em = discord.Embed(
                        title="⚔️ Coup du Rōnin",
                        description=(
                            f"Le Rōnin a frappé. **{target.display_name}** est éliminé.\n"
                            f"*Était-il coupable ? Ou le Rōnin a-t-il fait une erreur fatale ?*"
                        ),
                        color=0xe67e22,
                    )
                    em.set_footer(text="Enquête ・ Meira")
                    await game.channel.send(embed=em)
                except discord.HTTPException:
                    pass

    # ─────────────────── PHASE 4 : DÉBAT ────────────────────

    @staticmethod
    async def run_debate(game: Game):
        """Phase de débat public de 2 minutes, avec rappel à mi-parcours."""
        game.phase = "DEBATE"
        debate_duration = 120  # 2 min
        game.debate_end_time = datetime.now(PARIS_TZ) + timedelta(seconds=debate_duration)

        culprit_name = game.get_member(game.culprit_id).display_name
        end_ts = int(game.debate_end_time.timestamp())

        em = discord.Embed(
            title="🗣️ Phase de débat",
            description=(
                f"Tous les indices ont été révélés.\n\n"
                f"💬 Discutez, accusez-vous, défendez-vous — **2 minutes de débat libre**.\n"
                f"⏰ Fin du débat : <t:{end_ts}:R>\n\n"
                f"*Après ça, le vote anonyme sera lancé.*"
            ),
            color=0xe67e22,
        )
        em.set_footer(text="Enquête ・ Meira")
        await game.channel.send(embed=em)

        # Rappel à mi-parcours
        await asyncio.sleep(debate_duration // 2)
        try:
            await game.channel.send(embed=info_embed(
                "⏳ Plus qu'une minute",
                "Dernières accusations avant le vote..."
            ))
        except discord.HTTPException:
            pass
        await asyncio.sleep(debate_duration // 2)

    # ─────────────────── PHASE 5 : VOTE ────────────────────

    @staticmethod
    async def run_vote(game: Game):
        """Vote anonyme par bouton. Chaque joueur non-éliminé a 1 vote."""
        game.phase = "VOTE"
        voters = [m for m in game.participants if m.id not in game.eliminated_ids]

        em = discord.Embed(
            title="🗳️ Vote final",
            description=(
                f"**Qui est le coupable ?**\n\n"
                f"Clique sur **Voter** pour faire ton choix.\n"
                f"Ton vote est **anonyme**. Le Notable (Shōnin) compte pour **2 voix**.\n\n"
                f"⏳ Tu as **60 secondes** pour voter."
            ),
            color=0xe91e63,
        )
        em.set_footer(text="Enquête ・ Meira")

        view = VoteView(game, timeout=60)
        msg = await game.channel.send(embed=em, view=view)
        view.message = msg

        await view.wait()

        # Comptage avec double voix pour le Shōnin
        tally = {}
        for voter_id, target_id in game.votes.items():
            weight = 1
            if game.roles_assignment.get(voter_id) == "chief":
                weight = 2
            tally[target_id] = tally.get(target_id, 0) + weight

        if not tally:
            game.final_accused = None
            return

        # Trouve le max — en cas d'égalité, on choisit au hasard parmi les premiers
        max_votes = max(tally.values())
        top = [uid for uid, v in tally.items() if v == max_votes]
        game.final_accused = random.choice(top)

    # ─────────────────── PHASE 6 : RÉSOLUTION ────────────────────

    @staticmethod
    async def run_resolution(game: Game):
        """Gère la phase de vote puis annonce les résultats et distribue les récompenses."""
        await GameManager.run_vote(game)
        game.phase = "RESOLUTION"
        game.ended_at = datetime.now(PARIS_TZ)

        culprit_member = game.get_member(game.culprit_id)
        culprit_role = ROLES["culprit"]

        # Détermine si le coupable a été trouvé
        culprit_caught = (game.final_accused == game.culprit_id)
        game.culprit_survived = not culprit_caught

        # Construction du récap des rôles
        role_recap_lines = []
        for m in game.participants:
            rkey = game.roles_assignment.get(m.id)
            if not rkey:
                continue
            r = ROLES[rkey]
            eliminated = " *(éliminé)*" if m.id in game.eliminated_ids else ""
            crown = " 👑" if m.id == game.culprit_id else ""
            role_recap_lines.append(f"{r['emoji']} **{m.display_name}** — {r['name_fr']}{eliminated}{crown}")

        # Embed principal du résultat
        if culprit_caught:
            title = "✅ Le coupable a été démasqué !"
            main_desc = (
                f"Justice est rendue à Meira.\n\n"
                f"🎭 Le coupable était : **{culprit_member.display_name}** (Yamikage)\n"
                f"🗳️ Il a été accusé par vote à la majorité."
            )
            color = 0x43b581
        elif game.final_accused is not None:
            accused_member = game.get_member(game.final_accused)
            title = "❌ Erreur judiciaire"
            main_desc = (
                f"Un innocent a été condamné à tort.\n\n"
                f"🎭 Le vrai coupable était : **{culprit_member.display_name}** (Yamikage)\n"
                f"💀 Vous avez accusé : **{accused_member.display_name if accused_member else 'personne'}**\n"
                f"Le Yamikage s'en tire libre."
            )
            color = 0xf04747
        else:
            title = "😶 Aucune condamnation"
            main_desc = (
                f"Personne n'a été désigné à temps. Le coupable s'enfuit dans la nuit.\n\n"
                f"🎭 Le coupable était : **{culprit_member.display_name}** (Yamikage)"
            )
            color = 0xf04747

        em = discord.Embed(title=title, description=main_desc, color=color)
        em.add_field(name="🎭 Tous les rôles", value="\n".join(role_recap_lines) or "—", inline=False)
        em.set_footer(text="Enquête ・ Meira")
        await game.channel.send(embed=em)

        # Distribution des récompenses et stats
        await GameManager.distribute_rewards(game, culprit_caught)

        # Sauvegarde l'historique
        try:
            participants_data = [
                {"id": str(m.id), "name": m.display_name, "role": game.roles_assignment.get(m.id)}
                for m in game.participants
            ]
            save_game_history(
                game.game_id, game.guild.id, game.channel.id, game.host.id,
                game.scenario["key"], game.size_mode, len(game.participants),
                game.culprit_id, game.culprit_survived,
                game.started_at.isoformat(), game.ended_at.isoformat(),
                participants_data,
            )
        except Exception as e:
            log.error(f"Échec sauvegarde historique partie {game.game_id} : {e}")

        game.phase = "ENDED"

    @staticmethod
    async def distribute_rewards(game: Game, culprit_caught: bool):
        """XP, stats, badges. Annonce les niveaux et badges débloqués."""
        vigilante_failed = getattr(game, "vigilante_failed", set())
        all_new_badges = {}  # user_id → list of badge_keys
        level_up_messages = []

        for m in game.participants:
            uid = m.id
            rkey = game.roles_assignment.get(uid)
            if not rkey:
                continue
            role = ROLES[rkey]

            # Tous les participants : +1 game_played
            increment_player_stat(uid, "games_played", 1)

            # Tracking spécifique du rôle joué
            if rkey == "culprit":
                increment_player_stat(uid, "times_culprit", 1)
            if rkey in ("detective", "oracle", "witness"):
                increment_player_stat(uid, "times_detective", 1)

            # Détermine si ce joueur a gagné
            won = False
            xp_gain = 30  # XP de base pour participation
            if role["camp"] == "culprit":
                # Camp coupable gagne si le coupable survit
                if not culprit_caught:
                    won = True
                    xp_gain = 150 if rkey == "culprit" else 100
                    if rkey == "culprit":
                        increment_player_stat(uid, "culprit_wins", 1)
            else:
                # Camp innocent gagne si le coupable est puni
                if culprit_caught and uid not in vigilante_failed:
                    won = True
                    xp_gain = 80
                    # Bonus pour le votant qui a voté juste
                    if game.votes.get(uid) == game.culprit_id:
                        xp_gain += 40
                        increment_player_stat(uid, "correct_accusations", 1)
                    elif uid in game.votes and game.votes[uid] != game.culprit_id:
                        increment_player_stat(uid, "wrong_accusations", 1)
                else:
                    # Innocent qui a perdu
                    if uid in game.votes and game.votes[uid] != game.culprit_id:
                        increment_player_stat(uid, "wrong_accusations", 1)
                    elif uid in game.votes and game.votes[uid] == game.culprit_id:
                        # A bien voté mais n'a pas réussi à convaincre
                        xp_gain += 25
                        increment_player_stat(uid, "correct_accusations", 1)

            if won:
                increment_player_stat(uid, "games_won", 1)

            # Marque dernière partie
            update_player_stats(uid, last_played=datetime.now(PARIS_TZ).isoformat())

            # Award XP (gère le level-up)
            new_level, leveled_up, new_class = await award_xp(uid, xp_gain, ctx=None)
            if leveled_up:
                level_up_messages.append(
                    f"📈 {m.mention} passe niveau **{new_level}**"
                    + (f" ・ **{new_class}**" if new_class else "")
                )

            # Check badges
            new_badges = check_and_award_badges(uid)
            if new_badges:
                all_new_badges[uid] = new_badges

        # Annonce level ups
        if level_up_messages:
            try:
                await game.channel.send(embed=discord.Embed(
                    title="📈 Progression",
                    description="\n".join(level_up_messages),
                    color=0xffd700,
                ).set_footer(text="Enquête ・ Meira"))
            except discord.HTTPException:
                pass

        # Annonce badges
        if all_new_badges:
            lines = []
            for uid, badges_list in all_new_badges.items():
                member = game.get_member(uid)
                if not member:
                    continue
                for bkey in badges_list:
                    b = BADGES[bkey]
                    lines.append(f"{b['emoji']} {member.mention} débloque **{b['name']}**")
            if lines:
                try:
                    await game.channel.send(embed=discord.Embed(
                        title="🏅 Badges débloqués",
                        description="\n".join(lines),
                        color=0xf1c40f,
                    ).set_footer(text="Enquête ・ Meira"))
                except discord.HTTPException:
                    pass


# ========================= VIEWS =========================

class RecruitingView(discord.ui.View):
    """Vue du recrutement : Rejoindre / Partir / Lancer (host) / Annuler (host)."""
    def __init__(self, game: Game, timeout=600):
        super().__init__(timeout=timeout)
        self.game = game
        self.message = None
        self.cancelled = False

    async def refresh(self):
        if self.message:
            try:
                await self.message.edit(
                    embed=GameManager.build_recruiting_embed(self.game),
                    view=self,
                )
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Rejoindre 🎯", style=discord.ButtonStyle.success)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.bot:
            return await interaction.response.send_message("Les bots ne peuvent pas jouer.", ephemeral=True)
        if is_bot_banned(interaction.user.id):
            return await interaction.response.send_message("Tu es banni du bot Enquête.", ephemeral=True)
        if interaction.user in self.game.participants:
            return await interaction.response.send_message("Tu es déjà inscrit.", ephemeral=True)
        if len(self.game.participants) >= 40:
            return await interaction.response.send_message("Partie complète (40 joueurs max).", ephemeral=True)

        self.game.participants.append(interaction.user)
        await interaction.response.send_message(
            f"✅ Tu as rejoint l'enquête ! ({len(self.game.participants)} joueurs)",
            ephemeral=True,
        )
        await self.refresh()

    @discord.ui.button(label="Partir 🚪", style=discord.ButtonStyle.secondary)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id == self.game.host.id:
            return await interaction.response.send_message(
                "Tu es l'hôte, tu ne peux pas partir. Utilise **Annuler** si tu veux stopper la partie.",
                ephemeral=True,
            )
        if interaction.user not in self.game.participants:
            return await interaction.response.send_message("Tu n'es pas inscrit.", ephemeral=True)
        self.game.participants.remove(interaction.user)
        await interaction.response.send_message("👋 Tu as quitté l'enquête.", ephemeral=True)
        await self.refresh()

    @discord.ui.button(label="Lancer ▶️", style=discord.ButtonStyle.primary)
    async def launch(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.game.host.id:
            return await interaction.response.send_message("Seul l'hôte peut lancer.", ephemeral=True)
        if len(self.game.participants) < 3:
            return await interaction.response.send_message(
                "Il faut au moins **3 joueurs** pour lancer.",
                ephemeral=True,
            )
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Annuler ❌", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.game.host.id and not has_min_rank(interaction.user.id, 3):
            return await interaction.response.send_message(
                "Seul l'hôte ou un Sys peut annuler.",
                ephemeral=True,
            )
        self.cancelled = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()


class ActionSelect(discord.ui.Select):
    """Sélecteur de cible pour une action secrète en DM."""
    def __init__(self, game: Game, actor: discord.Member, role_key: str, candidates):
        options = [
            discord.SelectOption(
                label=m.display_name[:80],
                value=str(m.id),
                description=f"ID: {m.id}"[:100],
            )
            for m in candidates[:25]  # Discord limite à 25 options
        ]
        super().__init__(
            placeholder="Choisis ta cible...",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.game = game
        self.actor = actor
        self.role_key = role_key

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.actor.id:
            return await interaction.response.send_message(
                "Ce n'est pas ton action.", ephemeral=True
            )
        target_id = int(self.values[0])
        role = ROLES[self.role_key]

        # Enregistre l'action dans le game
        self.game.current_actions[self.actor.id] = {
            "action": role["action"],
            "target": target_id,
        }
        # Marque les usages uniques
        if self.role_key == "oracle":
            self.game.oracle_used = True
        if self.role_key == "journalist":
            self.game.journalist_used = True
        if self.role_key == "vigilante":
            self.game.vigilante_used = True

        target_member = self.game.get_member(target_id)
        target_name = target_member.display_name if target_member else "Inconnu"

        for item in self.view.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"✅ Action enregistrée : **{target_name}**",
            view=self.view,
        )
        self.view.stop()


class ActionView(discord.ui.View):
    def __init__(self, game: Game, actor: discord.Member, role_key: str, candidates):
        super().__init__(timeout=45)
        self.add_item(ActionSelect(game, actor, role_key, candidates))


class VoteSelect(discord.ui.Select):
    """Sélecteur anonyme pour le vote final."""
    def __init__(self, game: Game, candidates):
        options = [
            discord.SelectOption(
                label=m.display_name[:80],
                value=str(m.id),
            )
            for m in candidates[:25]
        ]
        super().__init__(
            placeholder="Qui est le coupable ?",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.game = game

    async def callback(self, interaction: discord.Interaction):
        uid = interaction.user.id
        # Vérifs
        if uid not in [m.id for m in self.game.participants]:
            return await interaction.response.send_message(
                "Tu ne participes pas à cette partie.", ephemeral=True
            )
        if uid in self.game.eliminated_ids:
            return await interaction.response.send_message(
                "Tu as été éliminé, tu ne peux pas voter.", ephemeral=True
            )
        target_id = int(self.values[0])
        self.game.votes[uid] = target_id
        await interaction.response.send_message(
            "✅ Ton vote a été enregistré (anonyme).", ephemeral=True
        )


class VoteView(discord.ui.View):
    def __init__(self, game: Game, timeout=60):
        super().__init__(timeout=timeout)
        self.game = game
        self.message = None
        # Les candidats = tous les participants encore vivants
        candidates = [m for m in game.participants if m.id not in game.eliminated_ids]
        self.add_item(VoteSelect(game, candidates))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


# ========================= MENU DE LANCEMENT =========================

class SizePresetSelect(discord.ui.Select):
    """Choix du preset de taille au lancement de *enquete."""
    def __init__(self, host: discord.Member, scenario_key=None):
        options = []
        for key, preset in SIZE_PRESETS.items():
            options.append(discord.SelectOption(
                label=preset["label"],
                value=key,
                emoji=preset["emoji"],
                description=preset["description"][:100],
            ))
        super().__init__(
            placeholder="Choisis le format de ta partie...",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.host = host
        self.scenario_key = scenario_key

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message(
                "Seul celui qui a lancé `*enquete` peut choisir.", ephemeral=True
            )
        preset_key = self.values[0]
        preset = SIZE_PRESETS[preset_key]

        if preset_key == "custom":
            # Ouvre un modal pour le nombre exact
            await interaction.response.send_modal(
                CustomSizeModal(self.host, self.scenario_key)
            )
            # Désactive la vue
            for item in self.view.children:
                item.disabled = True
            try:
                await interaction.message.edit(view=self.view)
            except discord.HTTPException:
                pass
            self.view.stop()
            return

        # Preset : utilise le default
        player_count = preset["default"]
        for item in self.view.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"✅ Format sélectionné : **{preset['label']}** ({player_count} joueurs cible)",
            view=self.view,
        )
        self.view.stop()
        # Lance la partie
        ctx = await bot.get_context(interaction.message)
        ctx.author = self.host  # corrige l'auteur
        await GameManager.start_game(ctx, preset_key, player_count, self.scenario_key)


class SizeSelectView(discord.ui.View):
    def __init__(self, host: discord.Member, scenario_key=None, timeout=120):
        super().__init__(timeout=timeout)
        self.add_item(SizePresetSelect(host, scenario_key))


class CustomSizeModal(discord.ui.Modal, title="Partie personnalisée"):
    def __init__(self, host: discord.Member, scenario_key=None):
        super().__init__()
        self.host = host
        self.scenario_key = scenario_key
        self.count_input = discord.ui.TextInput(
            label="Nombre de joueurs cible",
            placeholder="Entre 3 et 40",
            required=True,
            max_length=2,
        )
        self.add_item(self.count_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            n = int(self.count_input.value.strip())
        except ValueError:
            return await interaction.response.send_message(
                "Il faut un nombre entier.", ephemeral=True
            )
        if n < 3 or n > 40:
            return await interaction.response.send_message(
                "Le nombre doit être entre 3 et 40.", ephemeral=True
            )
        await interaction.response.send_message(
            f"✅ Partie personnalisée : **{n} joueurs cible**",
            ephemeral=True,
        )
        ctx = await bot.get_context(interaction.message) if interaction.message else None
        if ctx is None:
            # On recrée un ctx minimaliste depuis l'interaction
            class _Fake:
                pass
            ctx = _Fake()
            ctx.author = self.host
            ctx.guild = interaction.guild
            ctx.channel = interaction.channel
            ctx.send = interaction.channel.send
            ctx.bot = bot
            ctx.message = None
        else:
            ctx.author = self.host
        await GameManager.start_game(ctx, "custom", n, self.scenario_key)


# ========================= COMMANDE *enquete =========================

@bot.command(name="enquete")
async def _enquete(ctx, scenario_key: str = None):
    """
    Lance une nouvelle enquête. Le host choisit d'abord le format (preset ou custom),
    puis les joueurs s'inscrivent via boutons.
    Optionnellement, on peut passer une clé de scénario (ex: *enquete ryokan).
    """
    if await check_ban(ctx):
        return
    # Rang minimum : MJ (2)
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed(
            "❌ Permission refusée", "**MJ+** requis pour lancer une enquête."
        ))
    if ctx.channel.id in active_games:
        return await ctx.send(embed=error_embed(
            "❌ Partie en cours",
            "Une enquête est déjà active dans ce salon."
        ))

    # Valide la clé de scénario si fournie
    if scenario_key:
        valid_keys = [s["key"] for s in SCENARIOS]
        if scenario_key not in valid_keys:
            return await ctx.send(embed=error_embed(
                "❌ Scénario inconnu",
                f"Scénarios valides : `{'`, `'.join(valid_keys)}`\n"
                f"Ou lance sans argument pour un scénario aléatoire."
            ))

    em = discord.Embed(
        title="🕵️ Nouvelle enquête",
        description=(
            f"**Hôte :** {ctx.author.mention}\n\n"
            f"Choisis le **format** de ta partie ci-dessous.\n"
            f"*Une fois le format choisi, les inscriptions s'ouvrent pour 10 min max.*"
        ),
        color=0x3498db,
    )
    em.set_footer(text="Enquête ・ Meira")
    view = SizeSelectView(ctx.author, scenario_key)
    await ctx.send(embed=em, view=view)


# ========================= COMMANDES UTILITAIRES =========================

@bot.command(name="scenarios")
async def _scenarios(ctx):
    """Liste tous les scénarios disponibles."""
    if await check_ban(ctx):
        return
    lines = [f"• `{s['key']}` — **{s['title']}**" for s in SCENARIOS]
    em = info_embed(
        f"📚 Scénarios disponibles ({len(SCENARIOS)})",
        "\n".join(lines) + f"\n\nLance un scénario précis : `{get_prefix_cached()}enquete <clé>`"
    )
    await ctx.send(embed=em)


@bot.command(name="roles")
async def _roles_cmd(ctx):
    """Liste tous les rôles du jeu avec leurs descriptions."""
    if await check_ban(ctx):
        return
    # On split en 2 embeds : camp coupable + camp innocent
    culprit_lines = []
    innocent_lines = []
    for key, role in ROLES.items():
        line = f"{role['emoji']} **{role['name']}** *({role['name_fr']})*"
        if role["camp"] == "culprit":
            culprit_lines.append(line)
        else:
            innocent_lines.append(line)

    em = discord.Embed(
        title="🎭 Rôles du jeu",
        color=embed_color(),
    )
    em.add_field(
        name=f"🎭 Camp Coupable ({len(culprit_lines)})",
        value="\n".join(culprit_lines),
        inline=False,
    )
    em.add_field(
        name=f"👤 Camp Innocent ({len(innocent_lines)})",
        value="\n".join(innocent_lines),
        inline=False,
    )
    em.set_footer(text=f"Enquête ・ Meira ・ {get_prefix_cached()}role <clé> pour plus de détails")
    await ctx.send(embed=em)


@bot.command(name="role")
async def _role_detail(ctx, role_key: str = None):
    """Affiche le détail d'un rôle précis."""
    if await check_ban(ctx):
        return
    if not role_key or role_key not in ROLES:
        valid = ", ".join(f"`{k}`" for k in ROLES.keys())
        return await ctx.send(embed=error_embed(
            "❌ Rôle inconnu",
            f"Rôles valides : {valid}"
        ))
    role = ROLES[role_key]
    em = discord.Embed(
        title=f"{role['emoji']} {role['name']}",
        description=(
            f"**Nom FR :** {role['name_fr']}\n"
            f"**Camp :** {'🎭 Coupable' if role['camp'] == 'culprit' else '👤 Innocent'}\n\n"
            f"{role['description']}"
        ),
        color=0xe74c3c if role["camp"] == "culprit" else 0x3498db,
    )
    if role.get("action_desc"):
        em.add_field(name="🎯 Action secrète", value=role["action_desc"], inline=False)
    em.set_footer(text="Enquête ・ Meira")
    await ctx.send(embed=em)


@bot.command(name="abort")
async def _abort(ctx):
    """Force l'annulation d'une partie en cours (MJ+ uniquement)."""
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed(
            "❌ Permission refusée", "**MJ+** requis."
        ))
    game = active_games.get(ctx.channel.id)
    if not game:
        return await ctx.send(embed=error_embed(
            "❌ Aucune partie",
            "Aucune enquête n'est active dans ce salon."
        ))
    game.phase = "ENDED"
    active_games.pop(ctx.channel.id, None)
    await ctx.send(embed=success_embed(
        "✅ Partie annulée",
        "L'enquête a été annulée par un MJ."
    ))


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                 PARTIE 3 — STATS, ADMIN, HELP, RUN                       ║
# ╚══════════════════════════════════════════════════════════════════════════╝


# ========================= STATS / PROFIL / CLASSEMENT =========================

@bot.command(name="stats", aliases=["profil"])
async def _stats(ctx, *, user_input: str = None):
    """Affiche les stats d'un joueur (soi-même par défaut)."""
    if await check_ban(ctx):
        return

    target = ctx.author
    if user_input:
        resolved = await resolve_member(ctx, user_input)
        target = resolved if resolved else ctx.author

    stats = get_player_stats(target.id)
    role_counts = get_role_counts(target.id)
    badges_list = get_user_badges(target.id)

    # Niveau / XP / classe
    level = stats["level"]
    xp = stats["xp"]
    xp_curr_level = xp_for_level(level)
    xp_next_level = xp_for_level(level + 1) if level < 100 else xp
    xp_progress = xp - xp_curr_level
    xp_required = xp_next_level - xp_curr_level if level < 100 else 0
    current_class = class_for_level(level)
    next_class, next_class_lvl = next_class_info(level)

    # Ratios
    games = stats["games_played"]
    wins = stats["games_won"]
    winrate = f"{(wins/games*100):.1f}%" if games > 0 else "—"

    detect_total = stats["correct_accusations"] + stats["wrong_accusations"]
    detect_acc = f"{(stats['correct_accusations']/detect_total*100):.1f}%" if detect_total > 0 else "—"

    culprit_games = stats["times_culprit"]
    culprit_winrate = f"{(stats['culprit_wins']/culprit_games*100):.1f}%" if culprit_games > 0 else "—"

    # Rôle préféré
    fav_role_line = "*Aucun rôle joué*"
    if role_counts:
        top_key, top_count = role_counts[0]
        role = ROLES.get(top_key)
        if role:
            fav_role_line = f"{role['emoji']} **{role['name_fr']}** ({top_count}x)"

    # Description compacte (style CoinsBot)
    lines = [
        f"🎭 **{current_class}** ・ Niveau **{level}** / 100",
        f"✨ **{xp}** XP" + (f"  ・  *{xp_progress} / {xp_required}*" if level < 100 else "  ・  *MAX*"),
    ]
    if next_class:
        lines.append(f"🎯 Prochaine classe : **{next_class}** (niveau {next_class_lvl})")
    lines.append("")
    lines.append(f"🎲 **{games}** parties jouées  ・  🏆 **{wins}** gagnées ({winrate})")
    lines.append(f"🔍 **{stats['correct_accusations']}** bonnes accusations  ・  Précision : {detect_acc}")
    lines.append(f"🎭 **{culprit_games}** fois coupable  ・  **{stats['culprit_wins']}** survies ({culprit_winrate})")
    lines.append("")
    lines.append(f"⭐ Rôle préféré : {fav_role_line}")
    lines.append(f"🏅 Badges : **{len(badges_list)}** / {len(BADGES)}")

    em = discord.Embed(
        title=target.display_name,
        description="\n".join(lines),
        color=embed_color(),
    )
    em.set_thumbnail(url=target.display_avatar.url)
    em.set_footer(text=f"Enquête ・ Meira ・ {get_prefix_cached()}badges pour voir les badges")
    await ctx.send(embed=em)


@bot.command(name="badges")
async def _badges(ctx, *, user_input: str = None):
    """Affiche les badges débloqués d'un joueur."""
    if await check_ban(ctx):
        return

    target = ctx.author
    if user_input:
        resolved = await resolve_member(ctx, user_input)
        target = resolved if resolved else ctx.author

    owned = get_user_badges(target.id)
    owned_keys = {k for k, _ in owned}

    # On affiche tous les badges avec un marqueur
    lines = []
    unlocked_count = 0
    for bkey, bdata in BADGES.items():
        if bkey in owned_keys:
            lines.append(f"{bdata['emoji']} **{bdata['name']}** — *{bdata['desc']}*")
            unlocked_count += 1
        else:
            lines.append(f"🔒 ~~{bdata['name']}~~ — *{bdata['desc']}*")

    em = discord.Embed(
        title=f"🏅 Badges — {target.display_name}",
        description=(
            f"**{unlocked_count}** / {len(BADGES)} badges débloqués\n\n"
            + "\n".join(lines)
        ),
        color=embed_color(),
    )
    em.set_thumbnail(url=target.display_avatar.url)
    em.set_footer(text="Enquête ・ Meira")
    await ctx.send(embed=em)


@bot.command(name="classement", aliases=["lb", "leaderboard"])
async def _classement(ctx, metric: str = "xp"):
    """Affiche le classement du serveur. Métriques : xp, wins, detective, culprit, games."""
    if await check_ban(ctx):
        return

    metric_map = {
        "xp": ("xp", "✨ Classement XP", "XP"),
        "wins": ("games_won", "🏆 Classement Victoires", "victoires"),
        "games": ("games_played", "🎲 Classement Parties jouées", "parties"),
        "detective": ("correct_accusations", "🔍 Classement Détectives", "bonnes accusations"),
        "culprit": ("culprit_wins", "🎭 Classement Coupables", "survies en coupable"),
    }
    if metric not in metric_map:
        return await ctx.send(embed=error_embed(
            "❌ Métrique inconnue",
            f"Métriques disponibles : `{'`, `'.join(metric_map.keys())}`"
        ))

    db_field, title, label = metric_map[metric]
    top = get_leaderboard(db_field, limit=10)
    if not top:
        return await ctx.send(embed=info_embed(title, "*Aucun joueur classé*"))

    # Filtre aux membres du serveur quand possible (pour afficher les noms)
    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, (user_id, value) in enumerate(top):
        rank_marker = medals[i] if i < 3 else f"**{i+1}.**"
        member = ctx.guild.get_member(int(user_id)) if ctx.guild else None
        name = member.mention if member else f"<@{user_id}>"
        lines.append(f"{rank_marker} {name} ・ **{value}** {label}")

    em = discord.Embed(
        title=title,
        description="\n".join(lines),
        color=embed_color(),
    )
    em.set_footer(text=f"Enquête ・ Meira ・ {get_prefix_cached()}classement <xp|wins|games|detective|culprit>")
    await ctx.send(embed=em)


@bot.command(name="history")
async def _history(ctx):
    """Affiche l'historique des 10 dernières parties sur ce serveur."""
    if await check_ban(ctx):
        return

    games = get_recent_games(ctx.guild.id, limit=10)
    if not games:
        return await ctx.send(embed=info_embed(
            "📜 Historique",
            "Aucune partie enregistrée sur ce serveur."
        ))

    lines = []
    for g in games:
        scenario = next((s for s in SCENARIOS if s["key"] == g["scenario_key"]), None)
        scen_title = scenario["title"] if scenario else g["scenario_key"]
        outcome = "✅ coupable pris" if not g["culprit_survived"] else "❌ coupable échappé"
        try:
            ended = datetime.fromisoformat(g["ended_at"]).strftime("%d/%m %Hh%M")
        except (ValueError, TypeError):
            ended = "?"
        culprit_mention = f"<@{g['culprit_id']}>" if g['culprit_id'] else "?"
        lines.append(
            f"**{ended}** — *{scen_title}* ・ {g['player_count']}j ・ {outcome}\n"
            f"    🎭 Coupable : {culprit_mention}"
        )

    em = discord.Embed(
        title=f"📜 Dernières parties ({len(games)})",
        description="\n\n".join(lines),
        color=embed_color(),
    )
    em.set_footer(text="Enquête ・ Meira")
    await ctx.send(embed=em)


# ========================= ADMIN : RANGS =========================

@bot.command(name="sys")
async def _sys(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 4):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
        ids = get_ranks_by_level(3)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Sys", "Aucun sys."))
        return await ctx.send(embed=info_embed(
            f"📋 Liste Sys ({len(ids)})", "\n".join(f"<@{uid}>" for uid in ids)
        ))
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) == 3:
        return await ctx.send(embed=error_embed("Déjà Sys", f"{format_user_display(display, uid)} est déjà sys."))
    set_rank_db(uid, 3)
    await ctx.send(embed=success_embed("✅ Sys ajouté", f"{format_user_display(display, uid)} est maintenant **sys**."))


@bot.command(name="unsys")
async def _unsys(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Buyer** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 3:
        return await ctx.send(embed=error_embed("Pas Sys", f"{format_user_display(display, uid)} n'est pas sys."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Sys retiré", f"{format_user_display(display, uid)} n'est plus sys."))


@bot.command(name="mj")
async def _mj(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(2)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste MJ", "Aucun MJ."))
        return await ctx.send(embed=info_embed(
            f"📋 Liste MJ ({len(ids)})", "\n".join(f"<@{uid}>" for uid in ids)
        ))
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) >= 3:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur."))
    set_rank_db(uid, 2)
    await ctx.send(embed=success_embed("✅ MJ ajouté", f"{format_user_display(display, uid)} est maintenant **MJ**."))


@bot.command(name="unmj")
async def _unmj(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 2:
        return await ctx.send(embed=error_embed("Pas MJ", f"{format_user_display(display, uid)} n'est pas MJ."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ MJ retiré", f"{format_user_display(display, uid)} n'est plus MJ."))


@bot.command(name="joueur")
async def _joueur(ctx, *, user_input: str = None):
    """Ajoute un joueur vérifié (rang 1), permet à un MJ+ de flaguer les fiables."""
    if user_input is None:
        if not has_min_rank(ctx.author.id, 2):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**MJ+** requis."))
        ids = get_ranks_by_level(1)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Joueurs vérifiés", "Aucun."))
        return await ctx.send(embed=info_embed(
            f"📋 Joueurs vérifiés ({len(ids)})", "\n".join(f"<@{uid}>" for uid in ids)
        ))
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**MJ+** requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) >= 2:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur."))
    set_rank_db(uid, 1)
    await ctx.send(embed=success_embed("✅ Joueur vérifié", f"{format_user_display(display, uid)} est maintenant **joueur vérifié**."))


@bot.command(name="unjoueur")
async def _unjoueur(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**MJ+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if get_rank_db(uid) != 1:
        return await ctx.send(embed=error_embed("Pas vérifié", f"{format_user_display(display, uid)} n'est pas joueur vérifié."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Vérification retirée", f"{format_user_display(display, uid)} n'est plus vérifié."))


# ========================= ADMIN : BAN BOT =========================

@bot.command(name="ban")
async def _ban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if is_bot_banned(uid):
        return await ctx.send(embed=error_embed("Déjà banni", f"{format_user_display(display, uid)} est déjà banni."))
    add_bot_ban(uid, ctx.author.id)
    await ctx.send(embed=success_embed("⛔ Banni du bot", f"{format_user_display(display, uid)} ne peut plus utiliser Enquête."))


@bot.command(name="unban")
async def _unban(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))
    if not is_bot_banned(uid):
        return await ctx.send(embed=error_embed("Pas banni", f"{format_user_display(display, uid)} n'est pas banni."))
    remove_bot_ban(uid)
    await ctx.send(embed=success_embed("✅ Débanni", f"{format_user_display(display, uid)} peut à nouveau utiliser Enquête."))


# ========================= ADMIN : ALLOWED CHANNELS =========================

async def _resolve_channel(ctx, channel_input):
    clean = channel_input.strip("<#>")
    try:
        cid = int(clean)
        ch = ctx.guild.get_channel(cid)
        return ch, cid
    except ValueError:
        pass
    try:
        ch = await commands.TextChannelConverter().convert(ctx, channel_input)
        return ch, ch.id
    except commands.CommandError:
        return None, None


@bot.command(name="allow")
async def _allow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))

    if channel_input is None:
        allowed = get_allowed_channels(ctx.guild.id)
        if not allowed:
            return await ctx.send(embed=info_embed(
                "📋 Aucun salon autorisé",
                f"Personne ne peut utiliser le bot en dehors des **Sys+**.\n"
                f"Utilise `{get_prefix_cached()}allow #salon` pour en ajouter un."
            ))
        lines = []
        for cid in allowed:
            ch = ctx.guild.get_channel(int(cid))
            lines.append(f"• {ch.mention} (`{cid}`)" if ch else f"• *Salon inaccessible* (`{cid}`)")
        return await ctx.send(embed=info_embed(
            f"📋 Salons autorisés ({len(allowed)})", "\n".join(lines)
        ))

    channel, raw_id = await _resolve_channel(ctx, channel_input)
    if not channel:
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Mention `#salon` ou ID."))
    if is_channel_allowed(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed("Déjà autorisé", f"{channel.mention} est déjà autorisé."))
    add_allowed_channel(ctx.guild.id, channel.id, ctx.author.id)
    await ctx.send(embed=success_embed(
        "✅ Salon autorisé",
        f"{channel.mention} est maintenant un salon autorisé pour Enquête."
    ))
    await send_log(ctx.guild, "Salon autorisé", ctx.author,
                   desc=f"Salon : {channel.mention} (`{channel.id}`)", color=0x43b581)


@bot.command(name="unallow")
async def _unallow(ctx, *, channel_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not channel_input:
        return await ctx.send(embed=error_embed(
            "Argument manquant",
            f"Usage : `{get_prefix_cached()}unallow #salon` ou ID"
        ))
    channel, raw_id = await _resolve_channel(ctx, channel_input)
    if not channel:
        if raw_id is not None:
            if remove_allowed_channel(ctx.guild.id, raw_id):
                return await ctx.send(embed=success_embed(
                    "✅ Salon retiré",
                    f"Salon `{raw_id}` retiré (salon inaccessible ou supprimé)."
                ))
            return await ctx.send(embed=error_embed("Pas dans la liste", f"Salon `{raw_id}` pas autorisé."))
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Mention ou ID."))
    if not remove_allowed_channel(ctx.guild.id, channel.id):
        return await ctx.send(embed=error_embed("Pas dans la liste", f"{channel.mention} pas autorisé."))
    await ctx.send(embed=success_embed("✅ Salon retiré", f"{channel.mention} n'est plus autorisé."))
    await send_log(ctx.guild, "Salon retiré", ctx.author,
                   desc=f"Salon : {channel.mention} (`{channel.id}`)", color=0xf04747)


# ========================= ADMIN : SYSTÈME =========================

@bot.command(name="prefix")
async def _prefix(ctx, new_prefix: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut changer le prefix."))
    if not new_prefix:
        return await ctx.send(embed=info_embed("Prefix actuel", f"`{get_prefix_cached()}`"))
    set_config("prefix", new_prefix)
    await ctx.send(embed=success_embed("✅ Prefix modifié", f"Nouveau prefix : `{new_prefix}`"))


@bot.command(name="setlog")
async def _setlog(ctx, channel: discord.TextChannel = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut définir les logs."))
    if not channel:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un salon."))
    set_log_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed("✅ Logs configurés", f"Logs dans {channel.mention}."))


@bot.command(name="resetstats")
async def _resetstats(ctx, *, user_input: str = None):
    """Reset complet des stats d'un joueur (Sys+ uniquement)."""
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    async with stats_lock:
        conn = get_db()
        conn.execute("DELETE FROM player_stats WHERE user_id = ?", (str(uid),))
        conn.execute("DELETE FROM role_counts WHERE user_id = ?", (str(uid),))
        conn.execute("DELETE FROM badges WHERE user_id = ?", (str(uid),))
        conn.commit()
        conn.close()

    await ctx.send(embed=success_embed(
        "✅ Stats reset",
        f"Toutes les stats de {format_user_display(display, uid)} ont été supprimées."
    ))
    await send_log(ctx.guild, "Reset stats", ctx.author,
                   desc=f"Cible : {format_user_display(display, uid)}", color=0xe67e22)


# ========================= HELP DYNAMIQUE =========================

# Structure : chaque commande a son rang minimum
# Rangs : 0 = Membre, 1 = Joueur vérifié, 2 = MJ, 3 = Sys, 4 = Buyer

HELP_CATEGORIES = {
    "jeu": {
        "emoji": "🎮",
        "label": "Jeu",
        "title": "🎮  Jeu",
        "items": [
            ("enquete [scenario]", "Lancer une enquête (MJ+)", 2),
            ("abort",              "Annuler la partie en cours (MJ+)", 2),
            ("scenarios",          "Liste des scénarios", 0),
            ("roles",              "Liste des rôles", 0),
            ("role <clé>",         "Détail d'un rôle", 0),
        ],
    },
    "profil": {
        "emoji": "👤",
        "label": "Profil",
        "title": "👤  Profil & Stats",
        "items": [
            ("stats [@user]",       "Voir les stats d'un joueur", 0),
            ("profil [@user]",      "Alias de stats", 0),
            ("badges [@user]",      "Voir les badges d'un joueur", 0),
            ("classement <metric>", "Leaderboard (xp/wins/games/detective/culprit)", 0),
            ("history",             "10 dernières parties du serveur", 0),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "👥  Permissions",
        "items": [
            ("joueur @u / unjoueur @u", "Gérer les joueurs vérifiés", 2),
            ("mj @u / unmj @u",         "Gérer les MJ", 3),
            ("sys @u / unsys @u",       "Gérer les Sys", 4),
        ],
    },
    "admin": {
        "emoji": "🔧",
        "label": "Admin",
        "title": "🔧  Admin",
        "items": [
            ("ban @u",         "Bannir du bot", 3),
            ("unban @u",       "Débannir du bot", 3),
            ("resetstats @u",  "Reset stats d'un joueur", 3),
        ],
    },
    "system": {
        "emoji": "⚙️",
        "label": "Système",
        "title": "⚙️  Système",
        "items": [
            ("allow #salon",   "Autoriser un salon pour le bot", 3),
            ("unallow #salon", "Retirer un salon autorisé", 3),
            ("allow",          "Lister les salons autorisés", 3),
            ("setlog #salon",  "Salon de logs", 4),
            ("prefix [new]",   "Changer le prefix", 4),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "📋  Hiérarchie",
        "min_rank": 2,  # Visible dès MJ
        "items": [],    # Contenu statique dans build_hierarchy_embed
    },
}


def help_accessible_items(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    return [(s, d) for (s, d, mr) in cat.get("items", []) if rank >= mr]


def help_category_visible(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    if "min_rank" in cat:
        return rank >= cat["min_rank"]
    return len(help_accessible_items(key, rank)) > 0


def build_help_category_embed(key, rank):
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[key]
    em = discord.Embed(title=cat["title"], color=embed_color())
    items = help_accessible_items(key, rank)
    if not items:
        em.description = "*Aucune commande accessible à ton rang.*"
    else:
        max_syntax = max(len(f"{p}{syntax}") for syntax, _ in items)
        lines = [
            f"{p}{syntax}".ljust(max_syntax + 2) + f"→ {desc}"
            for syntax, desc in items
        ]
        em.description = "```\n" + "\n".join(lines) + "\n```"
    em.set_footer(text="Enquête ・ Meira")
    return em


def build_help_hierarchy_embed(rank):
    em = discord.Embed(title="📋  Hiérarchie", color=embed_color())
    lines = ["```\nBuyer > Sys > MJ > Joueur vérifié > Tout le monde\n```\n"]
    levels = [
        (4, "👑 **Buyer**",          "Accès total : `*prefix`, `*setlog`, `*sys`/`*unsys`"),
        (3, "🔧 **Sys**",             "`*allow`/`*unallow`, `*ban`/`*unban`, `*mj`/`*unmj`, `*resetstats`"),
        (2, "🎭 **MJ**",              "`*enquete`, `*abort`, `*joueur`/`*unjoueur`"),
        (1, "✨ **Joueur vérifié**",   "Statut privilégié, identique aux membres sinon"),
        (0, "👤 **Tout le monde**",   "Voir stats, badges, classement, scénarios, rôles"),
    ]
    for lvl, name, desc in levels:
        marker = " ← **toi**" if lvl == rank else ""
        lines.append(f"> {name} — {desc}{marker}")
    em.description = "\n".join(lines)
    em.set_footer(text="Enquête ・ Meira")
    return em


def build_help_home_embed(rank):
    p = get_prefix_cached()
    em = discord.Embed(color=embed_color())
    em.set_author(name="Enquête ─ Panel d'aide")

    rank_label = rank_name(rank)
    intro = (
        f"```\n🕐  {get_french_time()}\n```\n"
        f"Bienvenue dans **Enquête**, le jeu de déduction à rôles cachés de Meira.\n\n"
        f"**Prefix :** `{p}` ・ **Ton rang :** {rank_label}\n\n"
    )

    category_descriptions = {
        "jeu":       "Lancer/gérer les parties, scénarios, rôles",
        "profil":    "Stats personnelles, badges, classement",
        "perms":     "Attribuer les rangs",
        "admin":     "Modération des joueurs",
        "system":    "Configuration du bot",
        "hierarchy": "Qui peut faire quoi",
    }
    visible = []
    for key, lbl in category_descriptions.items():
        if help_category_visible(key, rank):
            cat = HELP_CATEGORIES[key]
            visible.append(f"> {cat['emoji']} **{cat['label']}** — {lbl}")

    em.description = intro + ("\n".join(visible) if visible else "")
    em.set_footer(text="Enquête ・ Meira")
    return em


def build_help_embed_for(key, rank):
    if key == "home":
        return build_help_home_embed(rank)
    if key == "hierarchy":
        return build_help_hierarchy_embed(rank)
    return build_help_category_embed(key, rank)


class HelpDropdown(discord.ui.Select):
    def __init__(self, user_rank):
        self.user_rank = user_rank
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        for key, cat in HELP_CATEGORIES.items():
            if help_category_visible(key, user_rank):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        if key != "home" and not help_category_visible(key, self.user_rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_help_embed_for(key, self.user_rank), view=self.view
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, user_rank):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.user_rank = user_rank
        self.add_item(HelpDropdown(user_rank))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Ce menu n'est pas à toi. Fais `*help` pour voir le tien.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="help")
async def _help(ctx):
    rank = get_rank_db(ctx.author.id)
    view = HelpView(ctx.author.id, rank)
    await ctx.send(embed=build_help_home_embed(rank), view=view)


# ========================= RUN =========================

if __name__ == "__main__":
    try:
        log.info("Démarrage de Enquête...")
        bot.run(BOT_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur fatale au démarrage : {e}", exc_info=True)
        sys.exit(1)