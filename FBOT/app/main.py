# -*- coding: utf-8 -*-
"""
MotionHunter SaaS — многопользовательский мониторинг Telegram-каналов и авто-отклики.
- bot_client (Telethon): бот управления для админов и клиентов.
- clients_dict: словарь активных TelegramClient (юзерботов) для каждого клиента.
Логика: ключевые слова + глаголы запроса → при сомнении OpenAI (YES/NO).
"""

import os
import re
import json
import random
import asyncio
import logging
import pathlib
import uuid
import time
import hmac
import hashlib
from urllib.parse import parse_qsl
from datetime import datetime, date, timedelta
import io
import qrcode
from typing import Dict, Any, List, Iterable, Optional, Union
import db

from aiohttp import web
import aiohttp_cors
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.errors.rpcerrorlist import (
    PeerIdInvalidError,
    UsernameInvalidError,
    ChatWriteForbiddenError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    PeerFloodError,
    UserBannedInChannelError
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.utils import get_peer_id

# ===== OpenAI =====
OPENAI_AVAILABLE = True
try:
    from openai import AsyncOpenAI
except Exception:
    OPENAI_AVAILABLE = False

# ================== Конфиг ==================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()

WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()

STATE_FILE = os.getenv("STATE_FILE", "./data/state.json")
SESSIONS_DIR = os.getenv("SESSIONS_DIR", "./.sessions")
BOT_SESSION = os.getenv("BOT_SESSION_NAME", "bot.session")
AUDIT_FILE = os.getenv("AUDIT_FILE", "./data/audit.jsonl")
AVATARS_DIR = os.getenv("AVATARS_DIR", "./frontend/avatars")

MIN_DELAY = max(0, int(os.getenv("MIN_DELAY", "1")))
MAX_DELAY = max(MIN_DELAY, int(os.getenv("MAX_DELAY", "15")))
VERBOSE = os.getenv("VERBOSE", "0").lower() in {"1", "true", "yes"}

ADMIN_IDS = set()

HARDCODED_ADMIN_PHONES = {
    "+77024383624", "77024383624", "7024383624", "87024383624",
    "+77059816066", "77059816066", "7059816066", "87059816066",
    "+77769827077", "77769827077", "7769827077", "87769827077",
    "+77059836066", "77059836066", "7059836066", "87059836066"
}

DEFAULT_REPLY = (
    "Привет!\n\n"
    "Я Белек, AI creator | Motion graphic designer (Астана)\n"
    "7 лет занимаюсь дизайном и анимацией.\n\n"
    "Готов обсудить задачу и сроки — могу созвониться."
)
# WEBAPP_URL moved to config section

DEFAULT_KEYWORDS = [
    "ищем специалиста 2d", "ищем специалиста 3d", "ai creator", "ai креатор",
    "ищу моушн дизайнера", "2d аниматор", "moho", "after effects", "монтажёр", 
    "ии ролик", "ai видео", "motion designer", "моушн", "анимация", "blender", 
    "c4d", "cinema 4d", "vfx", "shorts", "reels", "инфографика",
    "бюджет", "сроки", "тз", "оплата", "hiring", "need"
]
DEFAULT_NEGATIVE_WORDS = [
    "ищу работу", "ищу заказы", "возьму заказ", "готов выполнить",
    "выполню", "сделаю дешево", "портфолио", "резюме",
    "ищу подработку", "ищу вакансию", "ищу проект", "ищу стажировку",
    "готов работать", "ищу клиентов",
    "looking for job", "looking for work", "available for work",
    "hire me", "seeking gigs", "open to work", "need clients", "resume", "cv",
]

DEFAULT_SYSTEM_PROMPT = (
    "You are a strict binary classifier. Answer only YES or NO.\n"
    "Decide if the message is a CLIENT REQUEST looking to hire a motion/animation specialist "
    "(2D/3D, motion graphics, video), as opposed to a person advertising themselves or looking for a job. "
    "Input language may be RU/KZ/EN."
)

OPENAI_ENABLED = (
    os.getenv("OPENAI_ENABLED", "1").lower() in {"1", "true", "yes"}
    and OPENAI_AVAILABLE
    and bool(os.getenv("OPENAI_API_KEY", "").strip())
)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("motionbot")

# --- Глобальный стейт ---
STATE_LOCK_FILE = f"{STATE_FILE}.lock"
state_lock = asyncio.Lock()
STATE_CACHE: Optional[Dict[str, Any]] = None

def ensure_dirs():
    pathlib.Path(SESSIONS_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(STATE_FILE) or ".").mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(AUDIT_FILE) or ".").mkdir(parents=True, exist_ok=True)

def preview(s: str, n=160):
    s = (s or "").replace("\n", " ")
    return (s[:n] + "…") if len(s) > n else s

def norm(s: str) -> str:
    s = (s or "").replace("ё", "е")
    return re.sub(r"\s+", " ", s).strip().lower()

def _dedup_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = x.strip().lower()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _parse_terms(blob: str) -> List[str]:
    x = blob.replace(",", "\n").replace(";", "\n")
    return _dedup_keep_order([p for p in x.splitlines() if p.strip()])

def _write_audit_sync(entry: Dict[str, Any]):
    ensure_dirs()
    with open(AUDIT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

async def write_audit(entry: Dict[str, Any]):
    await asyncio.to_thread(_write_audit_sync, entry)

# ================== Клиенты и Сессии ==================
if not API_ID or not API_HASH or not BOT_TOKEN:
    raise SystemExit("API_ID, API_HASH, BOT_TOKEN required in .env")

ensure_dirs()
bot_session_path = str(pathlib.Path(SESSIONS_DIR) / BOT_SESSION)
bot_client = TelegramClient(bot_session_path, API_ID, API_HASH)

# Словарь запущенных клиентских юзерботов: user_id (str) -> TelegramClient
user_clients: Dict[str, TelegramClient] = {}

# FSM для логина: user_id -> { "phone": str, "client": TelegramClient, "phone_code_hash": str }
auth_sessions: Dict[int, Dict[str, Any]] = {}

# Активные процессы массовой рассылки для предотвращения дублей
active_mailings = set()

# Кэш для ускорения работы watcher при большой нагрузке (300+ юзеров)
USER_CACHE: Dict[str, Any] = {}
CHANNELS_CACHE: Dict[str, List[Dict[str, Any]]] = {}

async def get_cached_user(uid_str: str):
    if uid_str in USER_CACHE:
        return USER_CACHE[uid_str]
    u_data = await db.get_user(uid_str)
    if u_data:
        USER_CACHE[uid_str] = u_data
    return u_data

async def get_cached_channels(uid_str: str):
    if uid_str in CHANNELS_CACHE:
        return CHANNELS_CACHE[uid_str]
    channels = await db.get_channels(uid_str)
    CHANNELS_CACHE[uid_str] = channels
    return channels

def invalidate_cache(uid_str: str):
    USER_CACHE.pop(uid_str, None)
    CHANNELS_CACHE.pop(uid_str, None)

def is_subscribed(u_data: Dict[str, Any]) -> bool:
    """Проверяет наличие активной подписки пользователя."""
    if not u_data:
        return False
    expires_at = u_data.get("expires_at")
    if not expires_at:
        return False
    now = datetime.now(db.TZ_KZ)
    # Если в базе дата без часового пояса, приводим 'now' к такому же виду для сравнения
    if expires_at.tzinfo is None:
        return expires_at > now.replace(tzinfo=None)
    return expires_at > now.astimezone(expires_at.tzinfo)

if OPENAI_ENABLED:
    try:
        openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        log.info(f"OpenAI ON ({OPENAI_MODEL})")
    except Exception as e:
        OPENAI_ENABLED = False
        log.warning(f"OpenAI init failed: {e}")

REQ_VERBS = re.compile(
    r"(ищем|ищу|нужен|нужна|нужны|требуется|в\s*поиск|на\s*проект|задача|ваканси|hiring|hire|need|seeking)",
    re.I,
)

async def openai_gate(text: str, user_prompt: str = None) -> bool:
    if not OPENAI_ENABLED:
        return False
    try:
        system = user_prompt.strip() if user_prompt else DEFAULT_SYSTEM_PROMPT
        resp = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": f"Message:\n{text}"}],
        )
        out = (resp.choices[0].message.content or "").strip().upper()
        return out.startswith("Y")
    except Exception as e:
        log.warning(f"OpenAI check failed: {e}")
        return False

async def ensure_join(client: TelegramClient, link_or_at: str) -> Optional[int]:
    # ... (existing code omitted for brevity in thought, but I will include it properly in the chunk)
    try:
        link = link_or_at.strip()
        if link.startswith("@"):
            uname = link[1:]
            ent = await client.get_entity(uname)
            try:
                await client(JoinChannelRequest(ent))
            except UserAlreadyParticipantError:
                pass
            ent = await client.get_entity(uname)
            return int(get_peer_id(ent))
        elif "t.me/+" in link or "joinchat/" in link:
            try:
                # Telethon get_entity often works for invite links if already joined
                ent = await client.get_entity(link)
                return int(get_peer_id(ent))
            except Exception:
                # If not joined, try to join
                hash_ = link.split("t.me/")[-1]
                hash_ = hash_.split("+", 1)[-1] if "+" in hash_ else hash_.rsplit("/", 1)[-1]
                try:
                    res = await client(ImportChatInviteRequest(hash_))
                    if res and getattr(res, "chats", None):
                        for ch in res.chats:
                            try: return int(get_peer_id(ch))
                            except Exception: continue
                except UserAlreadyParticipantError:
                    # If already participant but get_entity failed, we can't do much without more info
                    # but usually get_entity(link) works for joined users.
                    pass
                return None
        else:
            if link.startswith("https://t.me/"):
                link = link.split("https://t.me/")[-1].strip("/")
            ent = await client.get_entity(link)
            try:
                await client(JoinChannelRequest(ent))
            except UserAlreadyParticipantError:
                pass
            return int(get_peer_id(ent))
    except Exception as e:
        log.warning(f"Join failed for {link_or_at}: {e}")
        return None

async def download_user_avatar(uid_str: str) -> Optional[str]:
    """Скачивает аватар пользователя и возвращает путь к нему"""
    avatar_path = os.path.join(AVATARS_DIR, f"{uid_str}.jpg")
    
    # Кэш на 1 час
    if os.path.exists(avatar_path):
        mtime = os.path.getmtime(avatar_path)
        if datetime.now(db.TZ_KZ).timestamp() - mtime < 3600:
            return f"/avatars/{uid_str}.jpg"
            
    # Приоритет 1: Юзербот
    client = user_clients.get(uid_str)
    if client:
        try:
            path = await client.download_profile_photo("me", file=avatar_path)
            if path:
                log.info(f"Downloaded avatar via user client for {uid_str}")
                return f"/avatars/{uid_str}.jpg"
        except: pass

    # Приоритет 2: Бот-клиент
    try:
        if str(uid_str).isdigit():
            path = await bot_client.download_profile_photo(int(uid_str), file=avatar_path)
            if path:
                log.info(f"Downloaded avatar via BOT client for {uid_str}")
                return f"/avatars/{uid_str}.jpg"
    except Exception as e:
        log.warning(f"Failed to download avatar via bot client for {uid_str}: {e}")
        
    return None

# ================== Юзербот Ватчер ==================
# Эта функция создает обработчик конкретно для данного пользователя (uid)
def make_watcher_handler(uid_str: str):
    async def watcher(event):
        try:
            # key используется для логов и аудит записей (как в /status или в выводе)
            key = uid_str 
            u_data = await get_cached_user(uid_str)
            if not u_data or not u_data.get("enabled"):
                return
            
            # Check expiry (Strict: no access if expires_at is None or past)
            if not is_subscribed(u_data) and not await is_admin(uid_str):
                return

            if getattr(event, 'out', False):
                return  # Игнорируем исходящие

            if getattr(event, 'is_private', False):
                return  # Только группы и каналы

            raw = event.raw_text or ""
            text = raw.strip()
            if not text:
                return

            tnorm = norm(text)
            client = user_clients.get(uid_str)
            if not client:
                return

            chat = getattr(event, "chat", None)
            chat_title = getattr(chat, "title", "") or str(event.chat_id)
            chat_username = getattr(chat, "username", None)
            
            # Конструируем ссылку на сообщение
            msg_id = event.id
            msg_link = ""
            if chat_username:
                msg_link = f"https://t.me/{chat_username}/{msg_id}"
            else:
                peer_id = str(event.chat_id)
                if peer_id.startswith("-100"):
                    peer_id = peer_id[4:]
                elif peer_id.startswith("-"):
                    peer_id = peer_id[1:]
                msg_link = f"https://t.me/c/{peer_id}/{msg_id}"
            
            # Фильтрация по каналам
            user_channels = await get_cached_channels(uid_str)
            if user_channels:
                current_chat_id = event.chat_id
                relevant_channel = None
                for ch in user_channels:
                    cid = ch.get("channel_id")
                    clink = (ch.get("channel_link") or "").lower()
                    
                    # Сопоставление по ID (основной способ)
                    if cid is not None and int(cid) == current_chat_id:
                        relevant_channel = ch
                        break
                    
                    # Сопоставление по ссылке/username (резервный способ если ID нет в базе)
                    if chat_username and chat_username.lower() in clink:
                        relevant_channel = ch
                        break
                    
                    # Сопоставление по вхождению титула чата в ссылку (совсем крайний случай)
                    if chat_title and norm(chat_title) in clink:
                        relevant_channel = ch
                        break

                if not relevant_channel or not relevant_channel.get("enabled", True):
                    return
        
            # Если дошли сюда — этот канал либо в списке разрешенных, либо список пуст (поиск везде)
            log.info(f"[Watcher:{uid_str}] Incoming message from '{chat_title}' (ID: {event.chat_id})")

            keywords = u_data.get("keywords") or DEFAULT_KEYWORDS
            negwords = u_data.get("negative_words") or DEFAULT_NEGATIVE_WORDS
            
            kw_norm = [norm(k) for k in keywords]
            neg_norm = [norm(n) for n in negwords]

            has_kw = any(k in tnorm for k in kw_norm)
            if not has_kw:
                # log.debug(f"[Watcher:{uid_str}] No keywords found in: {text[:30]}...")
                return

            neg = any(n in tnorm for n in neg_norm)
            
            used_openai = False
            ai_ok = False
            ok = False
            reason = "none"

            if neg:
                ok = False
                reason = "negative_word_match"
                log.info(f"[Watcher:{uid_str}] Filtered by negative word")
            elif REQ_VERBS.search(tnorm):
                ok = True
                reason = "primary_pass"
                log.info(f"[Watcher:{uid_str}] Primary pass (verb match)")
            else:
                used_openai = True
                prompt = u_data.get("system_prompt")
                log.info(f"[Watcher:{uid_str}] Sending to OpenAI...")
                ai_ok = await openai_gate(text, user_prompt=prompt)
                ok = ai_ok
                reason = "openai_yes" if ai_ok else "openai_no"
                log.info(f"[Watcher:{uid_str}] OpenAI decision: {reason}")

            # --- Target Extraction Magic ---
            target = None
            sender = await event.get_sender()
            
            # 1. Ищем @username в самом тексте (часто пишут "писать сюда @user")
            m = re.search(r"@([A-Za-z0-9_]{4,32})", raw) or re.search(r"t\.me/([A-Za-z0-9_]{4,32})", raw, re.I)
            if m:
                target = f"@{m.group(1)}"
            
            # 2. Если в тексте не нашли, берем отправителя сообщения
            if not target and sender:
                if getattr(sender, "username", None):
                    target = f"@{sender.username}"
                else:
                    # Если юзер без юзернейма, Телетон позволяет слать по ID, если мы в одном чате
                    target = sender.id
            
            if ok:
                log.info(f"[Watcher:{uid_str}] Lead found! Target: {target}, Reason: {reason}")

            final = "skip"
            send_error = None

            if ok and target:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                await asyncio.sleep(delay)
                try:
                    reply_text = str(u_data.get("reply_text") or DEFAULT_REPLY)
                    media_files = [] # TODO: support media for auto-replies if needed
                    if media_files:
                        first, rest = media_files[0], media_files[1:]
                        await client.send_file(target, first, caption=reply_text)
                        for mf in rest:
                            try:
                                await client.send_file(target, mf)
                                await asyncio.sleep(0.5)
                            except: pass
                    else:
                        await client.send_message(target, reply_text)
                    final = "sent"
                    
                    # Сохраняем статистику и добавляем в базу рассылки
                    today_dt = datetime.now(db.TZ_KZ)
                    today_str = str(today_dt.date())
                    
                    # Fetch fresh data for update
                    current_u_data = await get_cached_user(uid_str)
                    if current_u_data:
                        current_daily_date = str(current_u_data.get("daily_date", ""))
                        current_daily_sent = current_u_data.get("daily_sent", 0)
                        
                        if current_daily_date != today_str:
                            await db.update_user_field(uid_str, "daily_date", today_dt.date())
                            await db.update_user_field(uid_str, "daily_sent", 1)
                        else:
                            await db.update_user_field(uid_str, "daily_sent", current_daily_sent + 1)
                    
                    invalidate_cache(uid_str)
                    await db.add_crm_contacts(uid_str, [target], source="Авто-сбор")
                    
                    # Уведомляем админов и самого пользователя
                    who = target if isinstance(target, str) else f"id:{target}"
                    notify_txt = (
                        f"✅ Отклик отправлен {who} от имени {u_data.get('phone')}\n"
                        f"📍 Чат: {chat_title}\n"
                        f"🔗 Сообщение: {msg_link}"
                    )
                    asyncio.create_task(notify_user_and_admins(uid_str, notify_txt))
                        
                except FloodWaitError as e: send_error = f"FloodWait {e.seconds}s"
                except (UserPrivacyRestrictedError, UserIsBlockedError): send_error = "privacy/blocked"
                except Exception as e: 
                    send_error = str(e)
                    log.error(f"[Watcher:{uid_str}] Error sending message: {e}")
            elif ok and not target:
                reason = "no_target"

            # Аудит
            await write_audit({
                "key": key,
                "uid": uid_str,
                "chat_id": int(event.chat_id),
                "chat_title": chat_title,
                "preview": preview(raw, 220),
                "primary": ok,
                "negative": bool(neg),
                "used_openai": used_openai,
                "openai_ok": ai_ok,
                "decision": final,
                "reason": reason,
                "target": target if isinstance(target, str) else (int(target) if target else None),
                "send_error": send_error,
            })

            if u_data.get("tap"):
                mark = "🟢" if final == "sent" else "🟡" if ok and final != "sent" else "⚪"
                txt = f"{mark} [{key}] {chat_title}\n▶ {preview(raw, 200)}\ndecision: {final} | target: {target} | err: {send_error}"
                asyncio.create_task(notify_user_and_admins(uid_str, txt, prefix="[Auditing]"))
        
        except Exception as e:
            log.error(f"[Watcher:{uid_str}] Critical watcher error: {e}", exc_info=True)

    return watcher

async def notify_user_and_admins(user_id: Union[str, int], text: str, prefix: str = "[SaaS]"):
    """
    Отправляет уведомление пользователю и дублирует всем администраторам.
    """
    uid_str = str(user_id)
    # 1. Отправляем пользователю
    if uid_str.replace("-", "").isdigit():
        try:
            await bot_client.send_message(int(uid_str), text)
        except Exception as e:
            log.debug(f"Failed to notify user {uid_str}: {e}")

    # 2. Дублируем админам (кроме случая, если админ и есть наш юзер)
    for ad_id in ADMIN_IDS:
        if str(ad_id) != uid_str:
            try:
                # Плавная отправка админам (Traffic Shaping)
                await asyncio.sleep(0.1)
                await bot_client.send_message(int(ad_id), f"{prefix} (User {uid_str}): {text}")
            except Exception as e:
                log.debug(f"Failed to notify admin {ad_id}: {e}")

# ================== Команды Бота Управления ==================
async def is_admin(user_id: Any) -> bool:
    """Проверяет, является ли пользователь администратором (по ID, префиксу или полю в БД)."""
    uid_str = str(user_id)
    
    # 1. По Telegram ID (из конфига)
    if uid_str.isdigit() and int(uid_str) in ADMIN_IDS:
        return True
        
    # 2. Прямая проверка номера или web_ префикса (из конфига)
    target_phone = uid_str.replace("web_", "")
    if target_phone in HARDCODED_ADMIN_PHONES or uid_str in HARDCODED_ADMIN_PHONES:
        return True

    # 3. Проверка в базе данных / кэше
    u_data = await get_cached_user(uid_str)
    if u_data and u_data.get('is_admin'):
        return True

    return False

AWAITING_PASSWORD = set()
ADMIN_PASSWORD = "Maidan is a brilliant and great man of the 21st century"

# --- Общие команды ---
@bot_client.on(events.NewMessage(pattern=r"^/start$"))
async def cmd_start(event):
    uid_str = str(event.sender_id)
    global ADMIN_IDS

    # Авто-админ для пустой базы
    if not ADMIN_IDS:
        await db.add_admin(event.sender_id)
        ADMIN_IDS.add(event.sender_id)
        log.info(f"Initial Admin Bootstrap: {event.sender_id}")
        await event.respond("Вы были автоматически назначены администратором (первый запуск)!")
    
    # Для админа
    if await is_admin(event.sender_id):
        return await event.respond(
            f"Привет, Администратор!\n/admin_help - список админских команд SaaS\n\n🌐 Твоя ссылка Web App:\n{WEBAPP_URL}"
        )
    
    # Для зарегистрированного пользователя
    user_data = await get_cached_user(uid_str)
    if user_data:
        return await event.respond(
            "Привет! Твой юзербот запущен.\n/help - список команд настройки твоего откликера."
        )

    # Для новенького
    from telethon import types
    await event.respond(
        "Приветствую в системе MotionHunter!\n\nДля авторизации твоего юзербота, нажми кнопку ниже.",
        buttons=[[types.KeyboardButton("🔐 Войти по QR-коду")]]
    )

# --- Админка ---
@bot_client.on(events.NewMessage(pattern=r"^/admin\s+verify$"))
async def cmd_verify(event):
    if await is_admin(event.sender_id):
        return await event.respond("Ты уже админ ✅")
    AWAITING_PASSWORD.add(event.sender_id)
    await event.respond("Введи пароль:")

@bot_client.on(events.NewMessage(pattern=r"^/add_user\s+\+(\d+)$"))
async def cmd_add_user(event):
    if not await is_admin(event.sender_id): return
    phone = f"+{event.pattern_match.group(1)}"
    await db.add_allowed_phone(phone)
    await event.respond(f"✅ Добавлен в доступ: {phone}")

@bot_client.on(events.NewMessage(pattern=r"^/state$"))
async def cmd_state(event):
    if not await is_admin(event.sender_id): return
    allowed = await db.get_allowed_phones()
    txt = "📋 Разрешенные номера:\n" + "\n".join(allowed) + "\n\n"
    txt += "👥 Активные юзеры:\n"
    uids = await db.get_all_uids()
    for uid in uids:
        udata = await get_cached_user(uid)
        if udata:
            txt += f"UID: {uid} | Phone: {udata.get('phone')} | ON: {udata.get('enabled')}\n"
    await event.respond(txt)

# --- Авторизация по контакту и коду (FSM) ---
@bot_client.on(events.NewMessage())
async def fsm_handler(event):
    sender_id = event.sender_id
    text = event.raw_text.strip()

    # Пароль админа
    if sender_id in AWAITING_PASSWORD:
        try: await event.delete()
        except: pass
        AWAITING_PASSWORD.remove(sender_id)
        if text == ADMIN_PASSWORD:
            await db.add_admin(sender_id)
            global ADMIN_IDS
            ADMIN_IDS.add(sender_id)
            await event.respond(f"Пароль верный! 🎉 Ты теперь админ. Твоя ссылка Web App:\n{WEBAPP_URL}")
        else:
            await event.respond("Неверный пароль 🚫.")
        return

    # Запрос QR-кода
    if text == "🔐 Войти по QR-коду":
        user_data = await get_cached_user(str(sender_id))
        if user_data:
            return await event.respond("Твой аккаунт уже авторизован. Введи /help")

        await event.respond("Генерирую QR-код для входа...")
        temp_session_path = str(pathlib.Path(SESSIONS_DIR) / f"{sender_id}_login")
        
        # Полностью удаляем старую временную сессию перед новым запросом
        for ext in ["", ".session", ".session-journal"]:
            try:
                if os.path.exists(temp_session_path + ext):
                    os.remove(temp_session_path + ext)
            except: pass
            
        client = TelegramClient(
            temp_session_path, 
            API_ID, 
            API_HASH,
            device_model="Desktop",
            system_version="Windows 11",
            app_version="4.6.1",
            lang_code="en",
            system_lang_code="en"
        )
        await client.connect()
        
        try:
            qr = await client.qr_login()
            
            # Генерируем изображение
            img = qrcode.make(qr.url)
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='PNG')
            img_byte_arr.name = 'qr_login.png'
            img_byte_arr.seek(0)
            
            msg = await event.respond(
                "Отсканируй этот QR-код камерой с телефона:\n"
                "1. Открой Telegram (на телефоне)\n"
                "2. Настройки ➡️ Устройства ➡️ Подключить устройство\n"
                "3. Наведи камеру на этот код.\n\n"
                "*(QR-код действителен 30 секунд)*",
                file=img_byte_arr
            )
            
            auth_sessions[sender_id] = {"client": client, "qr_msg": msg, "step_2fa": False}
            
            # Ждём сканирования параллельно, чтобы не блокировать бота
            asyncio.create_task(wait_for_qr_scan(sender_id, qr))
            
        except Exception as e:
            log.error(f"Ошибка генерации QR-кода: {e}")
            await event.respond(f"Ошибка запроса QR: {e}")
            await client.disconnect()
        return

    # Ввод 2FA пароля
    if sender_id in auth_sessions and auth_sessions[sender_id].get("step_2fa"):
        auth = auth_sessions[sender_id]
        client = auth["client"]
        try:
            await event.delete() # Удаляем пароль из чата
        except: pass
        try:
            await client.sign_in(password=text)
            me = await client.get_me()
            auth["phone"] = f"+{me.phone}" if getattr(me, "phone", None) else str(me.id)
            await finalize_login(sender_id)
        except Exception as e:
            await event.respond(f"Ошибка 2FA: {e}")
            del auth_sessions[sender_id]
            await client.disconnect()
        return



async def wait_for_qr_scan(sender_id: int, qr):
    auth = auth_sessions.get(sender_id)
    if not auth: return
    client = auth["client"]
    try:
        # Ожидаем пока пользователь отсканирует QR (или таймаут ~30 секунд)
        # Если wait() возвращает None, значит вход 100% успешен или нужен 2FA
        await qr.wait(60) # чуть с запасом
        
        # Проверяем успешность
        if await client.is_user_authorized():
            me = await client.get_me()
            phone = f"+{me.phone}" if getattr(me, "phone", None) else str(me.id)
            auth["phone"] = phone
            try: await auth["qr_msg"].delete()
            except: pass
            
            # Если это один из админских номеров - дадим админку
            if phone in HARDCODED_ADMIN_PHONES:
                if sender_id not in ADMIN_IDS:
                    await db.add_admin(sender_id)
                    ADMIN_IDS.add(sender_id)
                await bot_client.send_message(sender_id, "Узнал тебя, Создатель! Права администратора выданы 👑")
                
            await finalize_login(sender_id)
            return
            
    except SessionPasswordNeededError:
        try: await auth["qr_msg"].delete()
        except: pass
        auth["step_2fa"] = True
        await bot_client.send_message(sender_id, "Код отсканирован! Но требуется пароль двухфакторной аутентификации (2FA). Введите его:")
        return
    except Exception as e:
        log.error(f"Ошибка QR login для {sender_id}: {e}")
        try: await auth["qr_msg"].delete()
        except: pass
        await bot_client.send_message(sender_id, "Время действия QR-кода истекло или произошла ошибка. Запросите новый код.")
    
    # Ресурсная очистка при неудаче
    if sender_id in auth_sessions:
        del auth_sessions[sender_id]
    await client.disconnect()

async def finalize_login(user_id: int):
    auth = auth_sessions[user_id]
    client = auth["client"]
    session_str = client.session.save()
    uid_str = str(user_id)
    
    # Отключаем временный клиент и удаляем файл сессии
    await client.disconnect()
    try:
        temp_session_path = str(pathlib.Path(SESSIONS_DIR) / f"{user_id}_login.session")
        if os.path.exists(temp_session_path):
            os.remove(temp_session_path)
    except Exception as e:
        log.warning(f"Failed to remove temp session: {e}")
    
    # Preserve existing settings if it's a re-login
    existing_user = await get_cached_user(uid_str)
    def_kw = DEFAULT_KEYWORDS.copy()
    def_neg = DEFAULT_NEGATIVE_WORDS.copy()

    user_db_data = {
        "phone": auth["phone"],
        "session_string": session_str,
        "enabled": existing_user.get("enabled", False) if existing_user else False,
        "reply_text": existing_user.get("reply_text", DEFAULT_REPLY) if existing_user else DEFAULT_REPLY,
        "keywords": existing_user.get("keywords", def_kw) if existing_user else def_kw,
        "negative_words": existing_user.get("negative_words", def_neg) if existing_user else def_neg,
        "system_prompt": existing_user.get("system_prompt", DEFAULT_SYSTEM_PROMPT) if existing_user else DEFAULT_SYSTEM_PROMPT,
        "daily_sent": existing_user.get("daily_sent", 0) if existing_user else 0,
        "daily_date": existing_user.get("daily_date", datetime.now(db.TZ_KZ).date()) if existing_user else datetime.now(db.TZ_KZ).date(),
        "mail_limit": existing_user.get("mail_limit", 50) if existing_user else 50
    }
    await db.upsert_user(uid_str, user_db_data)
    
    await bot_client.send_message(user_id, "Успешная авторизация! 🎉\nВведи /help для настройки.")
    del auth_sessions[user_id]
    
    # Запускаем в фоне юзербота через StringSession
    new_client = TelegramClient(
        StringSession(session_str),
        API_ID,
        API_HASH,
        device_model="Desktop",
        system_version="Windows 11",
        app_version="4.6.1",
        lang_code="en",
        system_lang_code="en"
    )
    await new_client.connect()
    user_clients[uid_str] = new_client
    new_client.add_event_handler(make_watcher_handler(uid_str), events.NewMessage())
    # Не делаем client.run_until_disconnected(), так как он работает асинхронно пока существует event loop.

# --- Пользовательские команды ---
@bot_client.on(events.NewMessage(pattern=r"^/help$"))
async def cmd_user_help(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    txt = (
        "Команды твоего юзербота:\n"
        "/on — включить рассылку откликов\n"
        "/off — выключить\n"
        "/add_channel <ссылка|@username> — добавить канал для сканирования\n"
        "/add_group <ссылка|@username> — добавить группу (чат) для сканирования\n"
        "/list_channels — список каналов\n"
        "/set_reply <текст> — твой шаблон отклика\n"
        "/get_reply — текущий шаблон\n"
        "/add_kw слово1;слово2 — добавить ключевые слова\n"
        "/list_kw — список ключевых\n"
        "/add_bad_kw слово — негативные слова для фильтра\n"
        "/status — твой статус\n\n"
        "📢 CRM и Рассылка:\n"
        "/list_mail — посмотреть размер базы рассылки\n"
        "/add_mail <@user|id> — добавить в базу вручную\n"
        "/collect_dialogs — собрать переписки в CRM\n"
        "/set_mail_limit <число> — лимит отправки за один раз\n"
        "/run_mail <Текст> — запустить рассылку по базе"
    )
    await event.respond(txt)

@bot_client.on(events.NewMessage(pattern=r"^/status$"))
async def cmd_user_status(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    
    enabled = "✅ ВКЛЮЧЕН" if user_data.get("enabled") else "⏸️ ВЫКЛЮЧЕН"
    channels = await get_cached_channels(uid_str)
    ch_count = len(channels)
    kw_count = len(user_data.get("keywords", []))
    
    expires_at = user_data.get("expires_at")
    if expires_at:
        exp_str = expires_at.strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Доступ не оплачен ❌"

    txt = (
        f"📊 **Твой статус:**\n"
        f"Статус воркера: {enabled}\n"
        f"Подписка до: {exp_str}\n"
        f"Отслеживается каналов: {ch_count}\n"
        f"Ключевых слов: {kw_count}\n"
        f"Отправлено сегодня: {user_data.get('daily_sent', 0)}\n\n"
        f"Используй /help для настройки."
    )
    await event.respond(txt)

@bot_client.on(events.NewMessage(pattern=r"^/on$|^/off$"))
async def cmd_user_toggle(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    enable = event.pattern_match.group(0) == "/on"

    if enable and not is_subscribed(user_data) and not await is_admin(uid_str):
        return await event.respond("🚫 Доступ ограничен. Пожалуйста, оплатите подписку для запуска воркера.")

    await db.update_user_field(uid_str, "enabled", enable)
    invalidate_cache(uid_str)
    await event.respond(f"Рассылка {'ВКЛЮЧЕНА ✅' if enable else 'ВЫКЛЮЧЕНА ⏸️'}")

@bot_client.on(events.NewMessage(pattern=r"^/set_reply\s+([\s\S]+)$"))
async def cmd_user_set_reply(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    txt = event.pattern_match.group(1)
    await db.update_user_field(uid_str, "reply_text", txt)
    invalidate_cache(uid_str)
    await event.respond("Ваш шаблон отклика сохранен ✅")

@bot_client.on(events.NewMessage(pattern=r"^/get_reply$"))
async def cmd_user_get_reply(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    await event.respond(f"Твой шаблон:\n\n{user_data.get('reply_text')}")

@bot_client.on(events.NewMessage(pattern=r"^/add_kw\s+([\s\S]+)$"))
async def cmd_user_add_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    terms = _parse_terms(event.pattern_match.group(1))
    cur_kws = list(user_data.get("keywords", []))
    new_kws = _dedup_keep_order(cur_kws + terms)
    await db.update_user_field(uid_str, "keywords", new_kws)
    invalidate_cache(uid_str)
    await event.respond(f"Ключи добавлены. Всего: {len(new_kws)}")

@bot_client.on(events.NewMessage(pattern=r"^/list_kw$"))
async def cmd_user_list_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    kws = list(user_data.get("keywords", []))
    if not kws:
        return await event.respond("У вас нет ключевых слов.")
    await event.respond("📋 Ваши ключевые слова:\n" + "; ".join(kws))

@bot_client.on(events.NewMessage(pattern=r"^/add_bad_kw\s+([\s\S]+)$"))
async def cmd_user_add_bad_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    terms = _parse_terms(event.pattern_match.group(1))
    cur_bad = list(user_data.get("negative_words", []))
    new_bad = _dedup_keep_order(cur_bad + terms)
    await db.update_user_field(uid_str, "negative_words", new_bad)
    invalidate_cache(uid_str)
    await event.respond(f"Минус-слова добавлены. Всего: {len(new_bad)}")

@bot_client.on(events.NewMessage(pattern=r"^/list_bad_kw$"))
async def cmd_user_list_bad_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    bad = list(user_data.get("negative_words", []))
    if not bad:
        return await event.respond("У вас нет минус-слов.")
    await event.respond("📋 Ваши минус-слова:\n" + "; ".join(bad))

@bot_client.on(events.NewMessage(pattern=r"^/del_kw\s+([\s\S]+)$"))
async def cmd_user_del_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    term = event.pattern_match.group(1).strip()
    cur_kw = list(user_data.get("keywords", []))
    if term in cur_kw:
        cur_kw.remove(term)
        await db.update_user_field(uid_str, "keywords", cur_kw)
        invalidate_cache(uid_str)
        await event.respond(f"Слово '{term}' удалено ✅")
    else:
        await event.respond(f"Слово '{term}' не найдено в списке.")

@bot_client.on(events.NewMessage(pattern=r"^/del_bad_kw\s+([\s\S]+)$"))
async def cmd_user_del_bad_kw(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    term = event.pattern_match.group(1).strip()
    cur_bad = list(user_data.get("negative_words", []))
    if term in cur_bad:
        cur_bad.remove(term)
        await db.update_user_field(uid_str, "negative_words", cur_bad)
        invalidate_cache(uid_str)
        await event.respond(f"Минус-слово '{term}' удалено ✅")
    else:
        await event.respond(f"Минус-слово '{term}' не найдено в списке.")

@bot_client.on(events.NewMessage(pattern=r"^/add_channel\s+(.+)$"))
async def cmd_user_add_channel(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    
    link = event.pattern_match.group(1).strip()
    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("ОШИБКА: Твой юзербот сейчас оффлайн. Обратись к админу.")
    
    await event.respond(f"Добавляю: {link}\nПодписываюсь...")
    cid = await ensure_join(client, link)
    if cid:
        await db.add_channel(uid_str, link)
        invalidate_cache(uid_str)
        await event.respond("Готово! Канал добавлен и юзербот на него подписался ✅")
    else:
        await event.respond("Не удалось подписаться на этот канал 🚫")
@bot_client.on(events.NewMessage(pattern=r"^/add_group\s+(.+)$"))
async def cmd_user_add_group(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    
    link = event.pattern_match.group(1).strip()
    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("ОШИБКА: Твой юзербот сейчас оффлайн. Обратись к админу.")
    
    await event.respond(f"Добавляю группу: {link}\nПодписываюсь...")
    cid = await ensure_join(client, link)
    if cid:
        await db.add_channel(uid_str, link, ctype="group")
        invalidate_cache(uid_str)
        await event.respond("Готово! Группа добавлена ✅")
    else:
        await event.respond("Не удалось подписаться на эту группу 🚫")

@bot_client.on(events.NewMessage(pattern=r"^/list_channels$"))
async def cmd_user_list_channels(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    channels = await get_cached_channels(uid_str)
    if not channels:
        return await event.respond("У вас нет добавленных каналов.")
    await event.respond("📋 Ваши каналы:\n" + "\n".join(channels))

# --- CRM и Рассылка ---
@bot_client.on(events.NewMessage(pattern=r"^/collect_dialogs$"))
async def cmd_user_collect_dialogs(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    
    if not is_subscribed(user_data) and not await is_admin(uid_str):
        return await event.respond("🚫 Доступ ограничен. Сбор диалогов доступен только по подписке.")

    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("Ваш юзербот сейчас оффлайн. Сначала подключите его через /start или WebApp.")
    
    msg = await event.respond("Начинаю сбор личных диалогов... Это может занять пару минут ⏳")
    
    contacts = []
    try:
        async for dialog in client.iter_dialogs(limit=None):
            if dialog.is_user and not dialog.entity.bot:
                if dialog.entity.username:
                    contacts.append(f"@{dialog.entity.username}")
                elif dialog.entity.phone:
                    contacts.append(f"+{dialog.entity.phone}")
                else:
                    contacts.append(str(dialog.entity.id))
                    
        my_id = str((await client.get_me()).id)
        contacts = [c for c in contacts if str(c) != my_id]
        
        added_count = await db.add_crm_contacts(uid_str, contacts, source="Чат")
        count = await db.get_crm_count(uid_str)
        await msg.edit(f"✅ Сбор завершен!\nНайдено новых личных переписок: {added_count}\nВсего в CRM базе: {count} контактов.")
    except Exception as e:
        log.error(f"Error collecting dialogs for {uid_str}: {e}")
        await msg.edit(f"❌ Произошла ошибка при сборе диалогов: {e}")

@bot_client.on(events.NewMessage(pattern=r"^/add_mail\s+(.+)$"))
async def cmd_user_add_mail(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    target = event.pattern_match.group(1).strip()
    await db.add_crm_contacts(uid_str, [target], source="Авто-сбор")
    count = await db.get_crm_count(uid_str)
    await event.respond(f"✅ Добавлен в базу рассылки: {target}. Всего в базе: {count}")

@bot_client.on(events.NewMessage(pattern=r"^/list_mail$"))
async def cmd_user_list_mail(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    count = await db.get_crm_count(uid_str)
    limit = user_data.get("mail_limit", 50)
    await event.respond(f"👥 В базе рассылки сейчас {count} человек(а).\n⚙️ Лимит отправки: {limit}")

@bot_client.on(events.NewMessage(pattern=r"^/set_mail_limit\s+(\d+)$"))
async def cmd_user_set_mail_limit(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    limit = int(event.pattern_match.group(1))
    await db.update_user_field(uid_str, "mail_limit", limit)
    invalidate_cache(uid_str)
    await event.respond(f"✅ Лимит рассылки за один запуск установлен на: {limit}")

@bot_client.on(events.NewMessage(pattern=r"^/run_mail\s+([\s\S]+)$"))
async def cmd_user_run_mail(event):
    uid_str = str(event.sender_id)
    user_data = await get_cached_user(uid_str)
    if not user_data: return
    
    text = event.pattern_match.group(1)
    ml_data = await db.get_crm_contacts(uid_str)
    ml = [c['contact'] for c in ml_data]
    limit = int(user_data.get("mail_limit", 50))
    limit = min(limit, len(ml))
    
    if limit == 0:
        return await event.respond("❌ База рассылки пуста или лимит равен 0.")
        
    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("❌ Твой юзербот сейчас оффлайн.")

    if not is_subscribed(user_data) and not await is_admin(uid_str):
        return await event.respond("🚫 Доступ ограничен. Рассылка доступна только по подписке.")

    if uid_str in active_mailings:
        return await event.respond("❌ У вас уже запущена рассылка! Дождитесь ее завершения.")
        
    active_mailings.add(uid_str)
    try:
        targets_to_mail = ml[:limit]
        await event.respond(f"🚀 Запускаю рассылку для {limit} контактов из базы...\nПримерное время: ~{limit * 3} сек.")
        
        sent_count = 0
        err_count = 0
        deleted_count = 0
        for tgt in targets_to_mail:
            should_move = True
            try:
                await client.send_message(tgt, text)
                sent_count += 1
            except FloodWaitError as e:
                log.warning(f"FloodWait in mail {uid_str}: sleeping {e.seconds}s")
                await event.respond(f"⚠️ Telegram запросил паузу (FloodWait). Жду {e.seconds} сек...")
                await asyncio.sleep(e.seconds)
                err_count += 1
                should_move = False
            except PeerFloodError:
                log.error(f"PeerFloodError: Аккаунт {uid_str} получил спам-мут!")
                await notify_user_and_admins(uid_str, "⛔️ **КРИТИЧЕСКАЯ ОШИБКА:**\nTelegram выдал вам временный спам-мут (PeerFloodError). Вы не можете писать первыми неконтактам.\nРассылка экстренно остановлена для защиты аккаунта!")
                break
            except (ConnectionError, asyncio.TimeoutError):
                log.warning(f"Connection/Timeout Error for {uid_str}. Sleeping 15s...")
                await asyncio.sleep(15)
                # Попробуем еще раз этот же контакт или пойдем дальше, 
                # пока просто засчитаем за ошибку и сдвинем, чтобы не зависнуть насмерть
                err_count += 1
                should_move = False
            except Exception as e:
                err_msg = str(e).lower()
                clean_keywords = ["deleted", "deactivated", "blocked", "privacy", "invalid", "nobody", "not find", "mutual"]
                if any(k in err_msg for k in clean_keywords):
                    await db.delete_crm_contact(uid_str, tgt)
                    deleted_count += 1
                    should_move = False
                else:
                    err_count += 1
                log.warning(f"Mail loop error for {tgt}: {e}")
                
            if should_move:
                await db.move_to_end(uid_str, tgt)
                
            await asyncio.sleep(random.uniform(2, 5)) # Антибан задержка
        
        await notify_user_and_admins(uid_str, f"✅ Рассылка завершена!\nУспешно: {sent_count}\nОшибок: {err_count}\nУдалено (мертвых/закрытых): {deleted_count}\n\n*Отправленные контакты перенесены в конец очереди.*")
    finally:
        active_mailings.discard(uid_str)


# ================== Старт Системы ==================
async def start_all_clients():
    uids = await db.get_all_uids()
    log.info(f"Starting clients for {len(uids)} users from DB: {uids}")
    
    for uid_str in uids:
        log.info(f"Attempting to start client for UID: {uid_str}")
        data = await get_cached_user(uid_str)
        if not data:
            log.warning(f"No data found for UID {uid_str} in DB.")
            continue
        
        session_str = data.get("session_string")
        if session_str:
            log.info(f"Found session string for {uid_str}, connecting...")
            client = TelegramClient(
                StringSession(session_str), 
                API_ID, 
                API_HASH,
                device_model="Desktop",
                system_version="Windows 11",
                app_version="4.6.1",
                lang_code="en",
                system_lang_code="en"
            )
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    log.warning(f"Session for {uid_str} is dead (not authorized).")
                    continue
                user_clients[uid_str] = client
                client.add_event_handler(make_watcher_handler(uid_str), events.NewMessage())
                
                # Сохраняем имя пользователя
                try:
                    me = await client.get_me()
                    name = me.first_name or ""
                    if me.last_name: name += f" {me.last_name}"
                    await db.update_user_field(uid_str, "name", name)
                    await db.update_user_field(uid_str, "username", me.username)
                    invalidate_cache(uid_str)
                except Exception as me_err:
                    log.error(f"Failed to get_me for {uid_str}: {me_err}")
                
                log.info(f"Successfully started client for UID {uid_str}")
                # Плавный запуск (Staggered Startup): 0.5 сек пауза между юзерами
                await asyncio.sleep(0.5)
            except Exception as e:
                log.error(f"Failed to start client {uid_str}: {e}")
        else:
            log.warning(f"Session string for {uid_str} is empty in DB.")

async def daily_report_task():
    """Фоновая задача для отправки вечерних отчетов (в 21:00)"""
    log.info("Started daily report background task.")
    while True:
        now = datetime.now(db.TZ_KZ)
        if now.hour == 21 and now.minute == 0:
            today_str = now.strftime("%Y-%m-%d")
            uids = await db.get_all_uids()
            for uid_str in uids:
                udata = await get_cached_user(uid_str)
                if not udata: continue
                
                # Исправляем сравнение даты: udata['daily_date'] может быть объектом date
                user_date = str(udata.get("daily_date", ""))
                if user_date == today_str and udata.get("daily_sent", 0) > 0:
                    count = udata["daily_sent"]
                    if str(uid_str).isdigit():
                        try:
                            txt = f"📊 Вечерний отчет по откликам за сегодня: {count}"
                            await notify_user_and_admins(uid_str, txt, prefix="[Report]")
                            # Плавная рассылка отчетов: 0.3 сек между пользователями
                            await asyncio.sleep(0.3)
                        except: pass
            await asyncio.sleep(60) # Спим 1 минуту, чтобы не отправить дважды в 21:00
        else:
            await asyncio.sleep(30) # Проверяем каждые полминуты

# ================== Web Server (Mini App) ==================
def validate_webapp_data(init_data: str, bot_token: str) -> bool:
    try:
        if not init_data: return False
        parsed_data = dict(parse_qsl(init_data))
        if "hash" not in parsed_data: return False
        hash_val = parsed_data.pop("hash")
        
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        return calculated_hash == hash_val
    except Exception:
        return False

async def get_auth_user_id(request) -> Optional[str]:
    """Универсальный метод получения ID пользователя (MiniApp или Browser)"""
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    web_token = request.headers.get("X-Web-Token", "")
    
    # 1. Сначала Mini App (приоритет)
    if init_data and validate_webapp_data(init_data, BOT_TOKEN):
        try:
            parsed = dict(parse_qsl(init_data))
            user_json = json.loads(parsed.get("user", "{}"))
            return str(user_json.get("id"))
        except: pass
        
    # 2. Браузер (по токену)
    if web_token:
        uid = await db.get_uid_by_token(web_token)
        if uid:
            log.debug(f"Auth: UID {uid} found via web_token")
            return uid
        else:
            log.warning(f"Auth: Token {web_token[:8]}... invalid or expired")
        
    return None

# Хранилище сессий сканирования через WebApp
webapp_qr_sessions = {}

routes = web.RouteTableDef()

@routes.get("/")
async def handle_index(request):
    return web.FileResponse('./frontend/index.html')

@routes.post("/api/qr_login")
async def api_qr_login(request):
    try:
        data = await request.json()
        sender_id = data.get("uid")
        
        # Если UID не передан, значит это "гость" из браузера
        is_guest = False
        if not sender_id:
            is_guest = True
            sender_id = random.randint(1000000, 9999999) # Временный ID для сессии QR
        else:
            sender_id = int(sender_id)

        str_sender_id = str(sender_id)
        
        # Для не-гостей проверяем авторизацию Init-Data
        if not is_guest:
            init_data = request.headers.get("X-Telegram-Init-Data", "")
            if not validate_webapp_data(init_data, BOT_TOKEN):
                return web.json_response({"error": "Unauthorized"}, status=401)
            
            user_data = await get_cached_user(str_sender_id)
            if user_data and user_data.get("session_string"):
                return web.json_response({"error": "Already registered"})

        # Уничтожаем старую сессию
        temp_session_path = str(pathlib.Path(SESSIONS_DIR) / f"{sender_id}_login")
        for ext in ["", ".session", ".session-journal"]:
            try:
                if os.path.exists(temp_session_path + ext):
                    os.remove(temp_session_path + ext)
            except: pass

        # Создаем клиент через StringSession, чтобы .session.save() вернуло полную сессионную строку, а не путь к файлу
        client = TelegramClient(
            StringSession(""),
            API_ID,
            API_HASH,
            device_model="Desktop",
            system_version="Windows 11",
            app_version="4.6.1",
            lang_code="en",
            system_lang_code="en"
        )
        await client.connect()
        qr = await client.qr_login()
        
        # Генерируем ID сессии авторизации
        session_id = uuid.uuid4().hex
        log.info(f"Generated new QR session_id: {session_id} for UID: {sender_id}")
        webapp_qr_sessions[session_id] = {
            "client": client,
            "qr": qr,
            "uid": sender_id,
            "original_uid": sender_id,
            "status": "pending",
            "is_guest": is_guest,
            "created_at": time.time()
        }
        
        # Фоновая задача ожидания
        async def wait_worker(sid):
            w_session = webapp_qr_sessions.get(sid)
            if not w_session: return
            cli = w_session["client"]
            qr_obj = w_session["qr"]
            try:
                await qr_obj.wait(120) # ждем 2 минуты в WebApp
                if await cli.is_user_authorized():
                    me = await cli.get_me()
                    phone = f"+{me.phone}" if getattr(me, "phone", None) else str(me.id)
                    w_session["phone"] = phone
                    w_session["uid"] = me.id # Реальный ID
                    w_session["name"] = (me.first_name or "") + (" " + me.last_name if me.last_name else "")
                    w_session["username"] = me.username
                    w_session["status"] = "success"
                else:
                    w_session["status"] = "failed"
            except SessionPasswordNeededError:
                w_session["status"] = "2fa_required"
            except asyncio.TimeoutError:
                log.info(f"QR Login Timeout for UID {sender_id}")
                w_session["status"] = "failed"
            except Exception as e:
                err_str = str(e) or repr(e)
                log.error(f"WA QR error: {err_str}")
                w_session["status"] = "failed"
                
        asyncio.create_task(wait_worker(session_id))
        
        return web.json_response({"status": "ok", "url": qr.url, "session_id": session_id})
    except Exception as e:
        log.error(f"QR WebApp Generation Error: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def finalize_webapp_login(w_session):
    """Общая логика завершения регистрации и запуска юзербота."""
    uid = w_session["uid"]
    client = w_session["client"]
    uid_str = str(uid)
    
    # Сохранение сессии
    session_str = client.session.save()
    await client.disconnect()

    # Очистка временных файлов
    try:
        orig_uid = w_session.get("original_uid", uid)
        temp_session_path = str(pathlib.Path(SESSIONS_DIR) / f"{orig_uid}_login")
        for ext in [".session", ".session-journal"]:
            if os.path.exists(temp_session_path + ext):
                os.remove(temp_session_path + ext)
    except: pass
    
    # Подготовка данных для БД
    user_phone = w_session.get("phone") or str(uid)
    # Preserve existing settings if it's a re-login
    existing_user = await get_cached_user(uid_str)
    def_kw = DEFAULT_KEYWORDS.copy()
    def_neg = DEFAULT_NEGATIVE_WORDS.copy()

    user_db_data = {
        "phone": user_phone,
        "session_string": session_str,
        "name": w_session.get("name"),
        "username": w_session.get("username"),
        "enabled": existing_user.get("enabled", False) if existing_user else False,
        "reply_text": existing_user.get("reply_text", DEFAULT_REPLY) if existing_user else DEFAULT_REPLY,
        "keywords": existing_user.get("keywords", def_kw) if existing_user else def_kw,
        "negative_words": existing_user.get("negative_words", def_neg) if existing_user else def_neg,
        "system_prompt": existing_user.get("system_prompt", DEFAULT_SYSTEM_PROMPT) if existing_user else DEFAULT_SYSTEM_PROMPT,
        "daily_sent": existing_user.get("daily_sent", 0) if existing_user else 0,
        "daily_date": existing_user.get("daily_date", datetime.now(db.TZ_KZ).date()) if existing_user else datetime.now(db.TZ_KZ).date(),
        "mail_limit": existing_user.get("mail_limit", 50) if existing_user else 50
    }
    await db.upsert_user(uid_str, user_db_data)
    invalidate_cache(uid_str)
    
    # Генерация веб-токена
    token = str(uuid.uuid4())
    await db.set_web_token(token, user_db_data["phone"])
    
    # Логика админа
    if user_phone in HARDCODED_ADMIN_PHONES:
        if uid not in ADMIN_IDS:
            await db.add_admin(uid)
            ADMIN_IDS.add(uid)
    
    log.info(f"User {uid_str} fully registered via WebApp (Finalize)")
    
    try:
        await bot_client.send_message(uid, "Успешная авторизация через WebApp! 🎉\nВведи /help для настройки.")
    except: pass
    
    # Запуск юзербота
    if uid_str in user_clients:
        try:
            log.info(f"Disconnecting old client for UID {uid_str} before re-auth")
            await user_clients[uid_str].disconnect()
        except: pass

    new_client = TelegramClient(
        StringSession(session_str), API_ID, API_HASH,
        device_model="Desktop", system_version="Windows 11", app_version="4.6.1",
        lang_code="en", system_lang_code="en"
    )
    await new_client.connect()
    
    if await new_client.is_user_authorized():
        user_clients[uid_str] = new_client
        new_client.add_event_handler(make_watcher_handler(uid_str), events.NewMessage())
    else:
        log.error(f"Failed to authorize new client for UID {uid_str} after finalize")
        return {"status": "error", "error": "Authorization failed after login"}
    
    return {
        "status": "success",
        "uid": uid_str,
        "name": w_session.get("name"),
        "username": w_session.get("username"),
        "web_token": token
    }

@routes.post("/api/qr_status")
async def api_qr_status(request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        w_session = webapp_qr_sessions.get(session_id)
        
        if not w_session:
            log.warning(f"QR Session {session_id} NOT FOUND in webapp_qr_sessions. Available: {list(webapp_qr_sessions.keys())}")
            return web.json_response({"error": "Session not found"}, status=404)
            
        status = w_session["status"]
        if status == "success":
            # Извлекаем и удаляем сессию
            w_session = webapp_qr_sessions.pop(session_id)
            resp_data = await finalize_webapp_login(w_session)
            return web.json_response(resp_data)
            
        elif status == "failed":
            client = w_session["client"]
            await client.disconnect()
            del webapp_qr_sessions[session_id]
            return web.json_response({"status": "failed"})
            
        elif status == "2fa_required":
            return web.json_response({"status": "2fa_required"})
            
        return web.json_response({"status": "pending"})
        
    except Exception as e:
        log.error(f"QR WebApp Status Error: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/2fa")
async def api_2fa_login(request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        password = data.get("password")
        
        w_session = webapp_qr_sessions.get(session_id)
        if not w_session:
            log.warning(f"2FA Session {session_id} NOT FOUND. Available: {list(webapp_qr_sessions.keys())}")
            return web.json_response({"error": "Session not found"}, status=404)
            
        client = w_session["client"]
        try:
            await client.sign_in(password=password)
            if await client.is_user_authorized():
                me = await client.get_me()
                w_session["uid"] = me.id
                w_session["phone"] = f"+{me.phone}" if getattr(me, "phone", None) else str(me.id)
                w_session["name"] = (me.first_name or "") + (" " + me.last_name if me.last_name else "")
                w_session["username"] = me.username
                
                # Завершаем регистрацию не дожидаясь опроса статуса
                webapp_qr_sessions.pop(session_id)
                resp_data = await finalize_webapp_login(w_session)
                return web.json_response(resp_data)
            else:
                return web.json_response({"error": "Unknown login failure"}, status=400)
        except Exception as e:
            err_msg = str(e)
            if "password is invalid" in err_msg.lower():
                err_msg = "Неверный пароль 2FA! Попробуйте еще раз."
            elif "too many attempts" in err_msg.lower():
                err_msg = "Слишком много попыток! Подождите немного."
            return web.json_response({"error": err_msg}, status=400)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)
async def maintenance_task():
    """Фоновая задача для обслуживания системы: ротация логов аудита и т.д."""
    while True:
        try:
            # Раз в час
            await asyncio.sleep(3600)
            
            # 1. Ротация файла аудита (ограничиваем 20МБ)
            if os.path.exists(AUDIT_FILE) and os.path.getsize(AUDIT_FILE) > 20 * 1024 * 1024:
                log.info("Rotating audit log file...")
                backup_path = f"{AUDIT_FILE}.old"
                if os.path.exists(backup_path):
                    os.remove(backup_path) # Удаляем совсем старый
                os.rename(AUDIT_FILE, backup_path)
                # Новый файл создастся автоматически при следующей записи
                
        except Exception as e:
            log.error(f"Error in maintenance_task: {e}")

async def webapp_qr_cleanup_task():
    """Фоновая задача для очистки старых QR/2FA сессий (TTL 5 минут)."""
    while True:
        try:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = []
            for sid, sdata in webapp_qr_sessions.items():
                # Если сессия старше 5 минут - удаляем
                if now - sdata.get("created_at", 0) > 300:
                    to_delete.append(sid)
            
            for sid in to_delete:
                log.info(f"🧹 Cleaning up expired QR session: {sid}")
                sdata = webapp_qr_sessions.pop(sid, None)
                if sdata and "client" in sdata:
                    try:
                        await sdata["client"].disconnect()
                    except: pass
        except Exception as e:
            log.error(f"Error in webapp_qr_cleanup_task: {e}")

@routes.get("/api/state")
async def api_get_state(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)

    udata = await get_cached_user(uid_str)
    if not udata or not udata.get("session_string"): 
        return web.json_response({"error": "User not registered", "registered": False}, status=200)
    
    # Попытаемся обновить аватарку в ответе
    avatar_url = await download_user_avatar(uid_str)
    
    expires_at = udata.get("expires_at")
    expired = False
    if expires_at:
        now = datetime.now(db.TZ_KZ)
        if expires_at.tzinfo is None:
            now = now.replace(tzinfo=None)
        else:
            now = now.astimezone(expires_at.tzinfo)
        
        if expires_at < now:
            expired = True

    resp = dict(udata)
    resp["avatar_url"] = avatar_url
    
    is_admin_flag = await is_admin(uid_str)
    resp["is_admin"] = is_admin_flag
    
    # Срок доступа
    if is_admin_flag:
        resp["expires_at"] = "Безлимит"
    else:
        exp = udata.get("expires_at")
        if isinstance(exp, (datetime, date)):
            resp["expires_at"] = exp.strftime("%d.%m.%Y %H:%M")
        else:
            resp["expires_at"] = "Доступ не оплачен"
        
    resp["expired"] = expired
    
    # Робастное преобразование всех дат для JSON
    for k, v in resp.items():
        if isinstance(v, (datetime, date)):
            resp[k] = v.isoformat()
            
    return web.json_response(resp)

@routes.post("/api/update")
async def api_update_state(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        data = await request.json()
        user_data = await get_cached_user(uid_str)
        if not user_data: return web.json_response({"error": "Not registered"}, status=404)
        
        if "enabled" in data:
            enable_val = bool(data["enabled"])
            # Администраторам подписка не требуется
            if enable_val and not is_subscribed(user_data) and not await is_admin(uid_str):
                return web.json_response({"error": "Subscription required to enable worker"}, status=403)
            await db.update_user_field(uid_str, "enabled", enable_val)
        if "reply_text" in data:
            await db.update_user_field(uid_str, "reply_text", str(data["reply_text"]))
        if "keywords" in data:
            await db.update_user_field(uid_str, "keywords", _dedup_keep_order(data["keywords"]))
        if "mail_limit" in data:
            await db.update_user_field(uid_str, "mail_limit", int(data["mail_limit"]))
        if "system_prompt" in data:
            await db.update_user_field(uid_str, "system_prompt", str(data["system_prompt"]))
            
        invalidate_cache(uid_str)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.get("/crm")
async def handle_crm(request):
    return web.FileResponse('./frontend/crm.html')

@routes.get("/profile")
async def handle_profile(request):
    return web.FileResponse('./frontend/profile.html')

@routes.get("/admin")
async def handle_admin(request):
    return web.FileResponse('./frontend/admin.html')

@routes.get("/api/profile")
async def api_get_profile(request):
    auth_uid = await get_auth_user_id(request)
    if not auth_uid:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    target_uid = request.query.get("uid")
    if target_uid and target_uid != auth_uid:
        if not await is_admin(auth_uid):
            return web.json_response({"error": "Forbidden"}, status=403)
        uid_str = target_uid
    else:
        uid_str = auth_uid
    
    udata = await get_cached_user(uid_str)
    if not udata or not udata.get("session_string"): 
        return web.json_response({"error": "User not registered", "registered": False}, status=200)

    name = udata.get("name")
    username = udata.get("username")
    
    # Пытаемся получить данные
    client = user_clients.get(uid_str)
    
    # Check expiry
    expires_at = udata.get("expires_at")
    is_admin_flag = await is_admin(uid_str)
    
    if is_admin_flag:
        expires_str = "Безлимит"
    else:
        expires_str = expires_at.strftime("%Y-%m-%d %H:%M") if expires_at else "Безлимит"
    if not name or name == "Пользователь":
        updated = False
        # Приоритет 1: Юзербот (дает макс данных)
        if client:
            try:
                log.debug(f"Fetching name from client for {uid_str}...")
                me = await client.get_me()
                name = me.first_name or ""
                if me.last_name: name += f" {me.last_name}"
                username = me.username
                updated = True
            except Exception as e:
                log.warning(f"Failed to fetch name from user client {uid_str}: {e}")
        
        # Приоритет 2: Бот-клиент (если юзербот спит)
        if not updated and str(uid_str).isdigit():
            try:
                log.debug(f"Fetching data from BOT client for {uid_str}...")
                user_entity = await bot_client.get_entity(int(uid_str))
                name = user_entity.first_name or ""
                if user_entity.last_name: name += f" {user_entity.last_name}"
                username = user_entity.username
                updated = True
            except Exception as e:
                log.warning(f"Failed to fetch name from bot client {uid_str}: {e}")

        if updated:
            await db.update_user_field(uid_str, "name", name)
            await db.update_user_field(uid_str, "username", username)
            invalidate_cache(uid_str)
            log.info(f"Updated profile for {uid_str}: {name}")

    crm_count = await db.get_crm_count(uid_str)
    
    profile_data = {
        "uid": uid_str,
        "name": name or "Пользователь",
        "username": username,
        "phone": udata.get("phone", "Unknown"),
        "is_admin": is_admin_flag,
        "daily_sent": udata.get("daily_sent", 0),
        "total_crm": crm_count,
        "avatar_url": await download_user_avatar(uid_str),
        "expires_at": expires_str,
        "expired": (expires_at < datetime.now(db.TZ_KZ).replace(tzinfo=expires_at.tzinfo)) if expires_at and expires_at.tzinfo else (expires_at < datetime.now(db.TZ_KZ).replace(tzinfo=None) if expires_at else False),
        "version": "1.2.0"
    }
    return web.json_response(profile_data)

@routes.get("/api/crm")
async def api_crm_list(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)

    udata = await get_cached_user(uid_str)
    if not udata or not udata.get("session_string"): 
        return web.json_response({"error": "User not registered", "registered": False}, status=200)

    q = request.query.get("q", "").strip()
    ml = await db.get_crm_contacts(uid_str, query=q)
    total = await db.get_crm_count(uid_str)
    return web.json_response({"contacts": ml, "total": total})

@routes.post("/api/crm/add")
async def api_crm_add(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        contacts_to_add = []
        
        # Поддержка как одного контакта, так и списка
        if "contacts" in data and isinstance(data["contacts"], list):
            contacts_to_add = [str(c).strip() for c in data["contacts"] if str(c).strip()]
        elif "contact" in data:
            c = str(data["contact"]).strip()
            if c: contacts_to_add = [c]

        if not contacts_to_add:
            return web.json_response({"error": "No contacts provided"}, status=400)

        user_data = await get_cached_user(uid_str)
        if not user_data: return web.json_response({"error": "Not registered"}, status=404)
        
        added = await db.add_crm_contacts(uid_str, contacts_to_add, source="Ручной ввод")
        total = await db.get_crm_count(uid_str)
        
        return web.json_response({"status": "ok", "added": added, "total": total})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)



@routes.get("/api/crm/export")
async def api_crm_export(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
        
    try:
        contacts = await db.get_crm_contacts(uid_str)
        if not contacts:
            return web.json_response({"error": "No contacts to export"}, status=404)
            
        output = io.StringIO()
        output.write("\ufeffКонтакт;Добавлено;Источник\n")
        for c in contacts:
            raw_date = c.get('created_at')
            if isinstance(raw_date, str):
                added_at = raw_date.replace('T', ' ').split('.')[0] # Убираем миллисекунды и T
            elif raw_date:
                added_at = raw_date.strftime("%Y-%m-%d %H:%M:%S")
            else:
                added_at = ""
            source = (c['source'] or "").replace('"', '""')
            contact = c['contact'].replace('"', '""')
            output.write(f'"{contact}";"{added_at}";"{source}"\n')
            
        content = output.getvalue()
        filename = f"crm_export_{uid_str}.csv"
        
        content_bytes = content.encode('utf-8')
        try:
            file_obj = io.BytesIO(content_bytes)
            file_obj.name = filename # КРИТИЧНО для Telethon, чтобы сохранить расширение
            
            await bot_client.send_file(
                int(uid_str), 
                file_obj, 
                caption="Ваш экспорт CRM базы 📂",
                force_document=True
            )
            return web.json_response({"status": "ok", "message": "sent_via_telegram"})
        except Exception as bot_err:
            log.error(f"Failed to send file via bot: {bot_err}")
            return web.json_response({"error": "Бот не смог отправить файл. Попробуйте позже."}, status=500)
    except Exception as e:
        log.error(f"CRM Export error: {e}")
        return web.json_response({"error": str(e)}, status=500)


@routes.get("/api/channels/export")
async def api_channels_export(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
        
    ctype = request.query.get("type", "channel").strip()
    
    try:
        channels = await db.get_channels(uid_str, ctype=ctype)
        if not channels:
            return web.json_response({"error": f"No {ctype}s to export"}, status=404)
            
        output = io.StringIO()
        output.write("\ufeffСсылка;ID;Статус;Тип\n")
        for ch in channels:
            link = (ch['channel_link'] or "").replace('"', '""')
            cid = str(ch['channel_id'] or "")
            status = "Включен" if ch['enabled'] else "Выключен"
            ct = "Группа" if (ch.get('type') == 'group' or ctype == 'group') else "Канал"
            output.write(f'"{link}";"{cid}";"{status}";"{ct}"\n')
            
        content = output.getvalue()
        type_label = "channels" if ctype == "channel" else "groups"
        filename = f"{type_label}_export_{uid_str}.csv"
        
        content_bytes = content.encode('utf-8')
        try:
            file_obj = io.BytesIO(content_bytes)
            file_obj.name = filename
            
            caption = "Ваш список каналов 📂" if ctype == "channel" else "Ваш список групп 📂"
            await bot_client.send_file(
                int(uid_str), 
                file_obj, 
                caption=caption,
                force_document=True
            )
            return web.json_response({"status": "ok", "message": "sent_via_telegram"})
        except Exception as bot_err:
            log.error(f"Failed to send file via bot: {bot_err}")
            return web.json_response({"error": "Бот не смог отправить файл. Попробуйте позже."}, status=500)
    except Exception as e:
        log.error(f"Channels Export error: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.get("/api/audit")
async def api_get_audit(request):
    auth_uid = await get_auth_user_id(request)
    if not auth_uid:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    target_uid = request.query.get("uid")
    if target_uid and target_uid != auth_uid:
        if not await is_admin(auth_uid):
            return web.json_response({"error": "Forbidden"}, status=403)
        uid_str = target_uid
    else:
        uid_str = auth_uid
        
    try:
        udata = await get_cached_user(uid_str)
        if not udata: 
            return web.json_response({"error": "User not registered"}, status=200)
            
        # Чтение логов пользователя из AUDIT_FILE
        records = []
        if os.path.exists(AUDIT_FILE):
            with open(AUDIT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        record = json.loads(line)
                        if str(record.get("uid")) == str(uid_str):
                            records.append(record)
                    except: pass
        
        records.reverse()
        return web.json_response({"records": records[:100]})
    except Exception as e:
        log.error(f"Audit API error: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/crm/delete")
async def api_crm_delete(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        contact = data.get("contact")
        if not contact:
            return web.json_response({"error": "No contact provided"}, status=400)
            
        user_data = await get_cached_user(uid_str)
        if not user_data: return web.json_response({"error": "Not registered"}, status=404)

        await db.delete_crm_contact(uid_str, contact)
        total = await db.get_crm_count(uid_str)
        return web.json_response({"status": "ok", "total": total})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.get("/api/admin/users")
async def api_admin_users(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str or not await is_admin(uid_str):
        return web.json_response({"error": "Forbidden"}, status=403)
    try:
        users = await db.get_all_users()
        # Clean up some data for security if needed
        for u in users:
            uid = u.get('uid')
            is_u_admin = await is_admin(uid)
            u['is_admin'] = is_u_admin # Передаем флаг для UI
                
            if is_u_admin:
                u['expires_at'] = "Безлимит"
            else:
                exp = u.get('expires_at')
                if isinstance(exp, (datetime, date)):
                    u['expires_at'] = exp.strftime("%d.%m.%Y %H:%M")
                else:
                    u['expires_at'] = "Доступ не оплачен"
            
            # Принудительно конвертируем все даты в строки для JSON
            for k in list(u.keys()):
                if isinstance(u[k], (datetime, date)):
                    u[k] = u[k].isoformat()
        return web.json_response({"users": users})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/admin/update_access")
async def api_admin_update_access(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str or not await is_admin(uid_str):
        return web.json_response({"error": "Forbidden"}, status=403)
    try:
        data = await request.json()
        phone = data.get("phone")
        months = data.get("months")
        if not phone or months is None:
            return web.json_response({"error": "Missing phone or months"}, status=400)
            
        new_expiry = await db.update_user_access(phone, int(months))
        if not new_expiry:
            return web.json_response({"error": "User not found or database pool error"}, status=404)
        
        # Инвалидируем кэш для ВСЕХ юзеров с этим UID (так как поиск по телефону)
        user_by_ph = await db.get_user_by_phone(phone)
        if user_by_ph and user_by_ph.get("uid"):
            invalidate_cache(user_by_ph["uid"])

        return web.json_response({"status": "ok", "new_expiry": new_expiry.strftime("%d.%m.%Y %H:%M")})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/admin/add_user")
async def api_admin_add_user(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str or not await is_admin(uid_str):
        return web.json_response({"error": "Forbidden"}, status=403)
    try:
        data = await request.json()
        phone = data.get("phone")
        months = int(data.get("months", 0))
        if not phone:
            return web.json_response({"error": "Missing phone"}, status=400)
        
        role = data.get("role", "user")
        await db.admin_add_user(phone, months, is_admin=(role == "admin"))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/collect_dialogs")
async def api_collect_dialogs(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    try:
        udata = await get_cached_user(uid_str)
        if not udata: return web.json_response({"error": "Not registered"}, status=404)
        
        if not is_subscribed(udata) and not await is_admin(uid_str):
            return web.json_response({"error": "Subscription required to collect dialogs"}, status=403)

        data = await request.json()
        ctype = data.get("type", "contacts") # "contacts" or "channels"
        
        client = user_clients.get(uid_str)
        if not client:
            return web.json_response({"error": "Userbot offline"}, status=400)
            
        results = []
        if ctype == "contacts":
            async for dialog in client.iter_dialogs(limit=None):
                if dialog.is_user and not dialog.entity.bot:
                    if dialog.entity.username:
                        results.append(f"@{dialog.entity.username}")
                    elif dialog.entity.phone:
                        results.append(f"+{dialog.entity.phone}")
                    else:
                        results.append(str(dialog.entity.id))
            
            my_id = str((await client.get_me()).id)
            results = [c for c in results if str(c) != my_id]
            added = await db.add_crm_contacts(uid_str, results, source="Диалоги")
            return web.json_response({"status": "ok", "added": added})
            
        elif ctype in ("channels", "groups"):
            channels_found = []
            groups_found = []
            async for dialog in client.iter_dialogs(limit=None):
                link = None
                if getattr(dialog.entity, 'username', None):
                    link = f"https://t.me/{dialog.entity.username}"
                
                if link:
                    if dialog.is_channel and not dialog.is_group:
                        channels_found.append(link)
                    elif dialog.is_group or (dialog.is_channel and dialog.is_group):
                        groups_found.append(link)
            
            added = 0
            if ctype == "channels":
                # Добавляем только каналы
                for l in channels_found:
                    if await db.add_channel(uid_str, l, ctype="channel"):
                        added += 1
            else:
                # ctype == "groups" — добавляем только группы
                for l in groups_found:
                    if await db.add_channel(uid_str, l, ctype="group"):
                        added += 1
                
            if added > 0:
                invalidate_cache(uid_str)
            return web.json_response({"status": "ok", "added": added})
            
        return web.json_response({"error": "Invalid type"}, status=400)
    except Exception as e:
        log.error(f"API collect error for {uid_str}: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.get("/api/channels")
async def api_channels_list(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        q = request.query.get("q", "").strip()
        ctype = request.query.get("type", "").strip() or None
        # Для списка каналов в API всё же лучше ходить в базу, так как там есть поиск/фильтры по типу
        channels = await db.get_channels(uid_str, query=q, ctype=ctype)
        return web.json_response({"channels": channels})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/channels/add")
async def api_channels_add(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        links = data.get("channels", [])
        if isinstance(data.get("channel"), str):
            links = [data["channel"]]
            
        if not links:
            return web.json_response({"error": "No channels provided"}, status=400)
            
        client = user_clients.get(uid_str)
        if not client:
            return web.json_response({"error": "User client offline"}, status=400)
            
        ctype = data.get("type", "channel")
        added_count = 0
        for link in links:
            link = link.strip()
            if not link: continue
            cid = await ensure_join(client, link)
            await db.add_channel(uid_str, link, channel_id=cid, ctype=ctype)
            added_count += 1
        
        if added_count > 0:
            invalidate_cache(uid_str)
                
        return web.json_response({"status": "ok", "added": added_count})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/channels/delete")
async def api_channels_delete(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        link = data.get("channel")
        if not link:
            return web.json_response({"error": "No channel provided"}, status=400)
            
        await db.remove_channel(uid_str, link)
        invalidate_cache(uid_str)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/channels/toggle")
async def api_channels_toggle(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        link = data.get("link")
        enabled = data.get("enabled")
        all_channels = data.get("all", False)

        if all_channels:
            await db.toggle_all_channels(uid_str, enabled)
        elif link is not None:
            await db.toggle_channel(uid_str, link, enabled)
        else:
            return web.json_response({"error": "Missing link or all flag"}, status=400)

        invalidate_cache(uid_str)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/channels/bulk_toggle")
async def api_channels_bulk_toggle(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
        channels = data.get("channels")
        if not isinstance(channels, list):
            return web.json_response({"error": "Invalid channels data"}, status=400)
            
        await db.bulk_toggle_channels(uid_str, channels)
        invalidate_cache(uid_str)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/mail")
async def api_run_mail(request):
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        data = await request.json()
        text = str(data.get("text"))
        provided_targets = data.get("targets") # Optional list of contacts
        
        udata = await get_cached_user(uid_str)
        if not udata: return web.json_response({"error": "Not registered"}, status=404)
        
        if isinstance(provided_targets, list) and len(provided_targets) > 0:
            targets = provided_targets
        else:
            ml_data = await db.get_crm_contacts(uid_str)
            ml = [item['contact'] for item in ml_data]
            limit = min(int(udata.get("mail_limit", 50)), len(ml))
            if limit == 0: return web.json_response({"error": "Base is empty or limit 0"}, status=400)
            targets = ml[:limit]
            
        if not targets:
            return web.json_response({"error": "No targets selected"}, status=400)
            
        client = user_clients.get(uid_str)
        if not client: return web.json_response({"error": "User client offline"}, status=400)
            
        if not is_subscribed(udata) and not await is_admin(uid_str):
            return web.json_response({"error": "Subscription required to run mailing"}, status=403)
            
        if uid_str in active_mailings:
            return web.json_response({"error": "Рассылка уже запущена! Дождитесь окончания."}, status=400)

        active_mailings.add(uid_str)
        
        async def background_mailer():
            try:
                s, e, deleted = 0, 0, 0
                for tgt in targets:
                    should_move = True
                    try:
                        await client.send_message(tgt, text)
                        s += 1
                    except FloodWaitError as fwe:
                        log.warning(f"FloodWait in API mail {uid_str}: sleeping {fwe.seconds}s")
                        await asyncio.sleep(fwe.seconds)
                        e += 1
                        should_move = False
                    except PeerFloodError:
                        log.error(f"PeerFloodError (Web API): Аккаунт {uid_str} получил спам-мут!")
                        await notify_user_and_admins(uid_str, "⛔️ **Web-Рассылка остановлена!**\nTelegram выдал вам спам-мут (PeerFloodError). Вы временно не можете писать неконтактам.")
                        break
                    except (ConnectionError, asyncio.TimeoutError):
                        log.warning(f"Connection/Timeout Error for {uid_str} in Web API. Sleeping 15s...")
                        await asyncio.sleep(15)
                        e += 1
                        should_move = False
                    except Exception as err:
                        err_str = str(err).lower()
                        clean_keywords = ["deleted", "deactivated", "blocked", "privacy", "invalid", "nobody", "not find", "mutual"]
                        if any(k in err_str for k in clean_keywords):
                            await db.delete_crm_contact(uid_str, tgt)
                            deleted += 1
                            should_move = False
                        else:
                            e += 1
                        log.warning(f"API mail loop err for {tgt}: {err}")
                    
                    if should_move:
                        await db.move_to_end(uid_str, tgt)
                        
                    await asyncio.sleep(random.uniform(2, 5))

                # Уведомление об окончании рассылки (вне цикла!)
                await notify_user_and_admins(uid_str, f"✅ Web-рассылка завершена!\nУспех: {s}\nОшибок: {e}\nУдалено мертвых: {deleted}")
            finally:
                active_mailings.discard(uid_str)
        
        try:
            asyncio.create_task(background_mailer())
            return web.json_response({"status": "ok", "message": f"Рассылка запущена для {limit} контактов"})
        except Exception as spawn_err:
            active_mailings.discard(uid_str)
            raise spawn_err

    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/logout")
async def api_logout(request):
    """Выход из Telegram: сбрасывает сессию юзербота, не удаляет аккаунт."""
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        # Останавливаем активного клиента
        client = user_clients.pop(uid_str, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Сбрасываем session_string в БД
        await db.clear_user_session(uid_str)
        invalidate_cache(uid_str)
        log.info(f"User {uid_str} logged out (session cleared)")
        return web.json_response({"status": "ok"})
    except Exception as e:
        log.error(f"Logout error for {uid_str}: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/account/delete")
async def api_account_delete(request):
    """Полное удаление аккаунта: удаляет все данные, останавливает юзербот."""
    uid_str = await get_auth_user_id(request)
    if not uid_str:
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        # Останавливаем юзербот
        client = user_clients.pop(uid_str, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Удаляем все данные из БД
        deleted = await db.delete_user(uid_str)
        if not deleted:
            return web.json_response({"error": "Пользователь не найден"}, status=404)
        log.info(f"User {uid_str} account DELETED")
        invalidate_cache(uid_str)
        return web.json_response({"status": "ok"})
    except Exception as e:
        log.error(f"Delete account error for {uid_str}: {e}")
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/admin/revoke_access")
async def api_admin_revoke_access(request):
    """Полное аннулирование доступа пользователю."""
    uid_str = await get_auth_user_id(request)
    if not uid_str or not await is_admin(uid_str):
        return web.json_response({"error": "Forbidden"}, status=403)
    try:
        data = await request.json()
        phone = data.get("phone")
        if not phone:
            return web.json_response({"error": "Missing phone"}, status=400)
            
        async with db.pool.acquire() as conn:
            await conn.execute("UPDATE users SET expires_at = NULL WHERE phone = $1", phone)
        
        user_by_ph = await db.get_user_by_phone(phone)
        if user_by_ph and user_by_ph.get("uid"):
            invalidate_cache(user_by_ph["uid"])
            
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

async def init_web_server():
    app = web.Application()
    app.add_routes(routes)
    
    # Раздача аватарок и статики
    if not os.path.exists(AVATARS_DIR): os.makedirs(AVATARS_DIR, exist_ok=True)
    app.router.add_static('/avatars/', path=AVATARS_DIR, name='avatars')
    app.router.add_static('/assets/', path='./frontend/assets', name='assets')
    
    # Enable CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True, expose_headers="*", allow_headers="*",
        )
    })
    for route in list(app.router.routes()):
        cors.add(route)
        
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    # Web App Notification
    if WEBAPP_URL:
        log.info(f"🚀 Web App is active at: {WEBAPP_URL}")
        
        async def safe_notify(ad_id, msg):
            try:
                await bot_client.send_message(int(ad_id), msg)
            except Exception as e:
                log.warning(f"Could not notify admin {ad_id}: {e}")

        for ad in ADMIN_IDS:
            asyncio.create_task(safe_notify(ad, f"🌐 Web App запущен на сервере:\n{WEBAPP_URL}"))
    else:
        log.warning("WEBAPP_URL is not set in .env! Users won't get the link.")

async def main():
    # 0. Инициализация БД
    try:
        await db.init_db()
        # Загружаем админов в кэш
        global ADMIN_IDS
        ADMIN_IDS = set(await db.get_admins())
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        return

    logging.getLogger("telethon").setLevel(logging.WARNING)
    log.info("Starting SaaS Control Bot...")
    await bot_client.start(bot_token=BOT_TOKEN)
    log.info("Starting Users clients...")
    await start_all_clients()
    
    asyncio.create_task(daily_report_task())
    asyncio.create_task(webapp_qr_cleanup_task())
    asyncio.create_task(maintenance_task())
    asyncio.create_task(init_web_server())
    
    log.info("System fully operational.")
    await bot_client.run_until_disconnected()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
