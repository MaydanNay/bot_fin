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
from datetime import datetime
from typing import Dict, Any, List, Iterable, Optional

from aiohttp import web
import aiohttp_cors
from pyngrok import ngrok
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

STATE_FILE = os.getenv("STATE_FILE", "./data/state.json")
SESSIONS_DIR = os.getenv("SESSIONS_DIR", "./.sessions")
BOT_SESSION = os.getenv("BOT_SESSION_NAME", "bot.session")

MIN_DELAY = max(0, int(os.getenv("MIN_DELAY", "1")))
MAX_DELAY = max(MIN_DELAY, int(os.getenv("MAX_DELAY", "15")))
VERBOSE = os.getenv("VERBOSE", "0").lower() in {"1", "true", "yes"}

DEFAULT_REPLY = (
    "Привет!\n\n"
    "Я Белек, AI creator | Motion graphic designer (Астана)\n"
    "7 лет занимаюсь дизайном и анимацией.\n\n"
    "Готов обсудить задачу и сроки — могу созвониться."
)
DEFAULT_KEYWORDS = [
    "ищем специалиста 2d", "ищем специалиста 3d", "ai creator", "ai креатор",
    "ищу моушн дизайнера", "2d аниматор", "moho", "after effects", "монтажёр", 
    "ии ролик", "ai видео", "motion designer", "моушн", "анимация", "blender", 
    "c4d", "cinema 4d", "vfx", "shorts", "reels", "инфографика",
    "бюджет", "сроки", "тз", "оплата", "hiring", "need"
]
DEFAULT_NEGATIVE_WORDS = [
    "ищу работу", "ищу заказы", "возьму заказ", "сделаю дешево", "портфолио", 
    "резюме", "ищу подработку", "ищу вакансию", "ищу проект"
]

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

# ================== Состояние/Аудит ==================
state_lock = asyncio.Lock()

# Структура state.json для SaaS:
# {
#   "admin_ids": [123456],
#   "allowed_phones": ["+77771234567"],
#   "users": {
#       "123456": {
#           "phone": "+77771234567",
#           "session_string": "1BJW...",
#           "enabled": true,
#           "tap": false,
#           "reply_text": "...",
#           "keywords": [...],
#           "negative_words": [...],
#           "channels": [...],
#           "media_files": [...],
#           "mailing_list": [...],
#           "daily_stats": {"date": "2026-02-23", "sent": 0},
#           "mail_limit": 50
#       }
#   }
# }

STATE_DEFAULT: Dict[str, Any] = {
    "admin_ids": [],
    "allowed_phones": [],
    "users": {}
}

AUDIT_FILE = "./data/audit.jsonl"
STATE_CACHE: Optional[Dict[str, Any]] = None

def ensure_dirs():
    pathlib.Path(SESSIONS_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(STATE_FILE) or ".").mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(AUDIT_FILE) or ".").mkdir(parents=True, exist_ok=True)

def load_state() -> Dict[str, Any]:
    global STATE_CACHE
    if STATE_CACHE is not None:
        return STATE_CACHE
    ensure_dirs()
    if not os.path.exists(STATE_FILE):
        STATE_CACHE = dict(STATE_DEFAULT)
        return STATE_CACHE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in STATE_DEFAULT.items():
            data.setdefault(k, v)
        STATE_CACHE = data
        return STATE_CACHE
    except Exception as e:
        log.warning(f"state load failed: {e}")
        STATE_CACHE = dict(STATE_DEFAULT)
        return STATE_CACHE

async def save_state(st: Dict[str, Any]):
    global STATE_CACHE
    ensure_dirs()
    STATE_CACHE = dict(st)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

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

async def openai_gate(text: str) -> bool:
    if not OPENAI_ENABLED:
        return False
    try:
        system = (
            "You are a strict binary classifier. Answer only YES or NO.\n"
            "Decide if the message is a CLIENT REQUEST looking to hire a motion/animation specialist "
            "(2D/3D, motion graphics, video), as opposed to a person advertising themselves or looking for a job. "
            "Input language may be RU/KZ/EN."
        )
        resp = await openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": f"Message:\n{text}"}],
            temperature=0,
        )
        out = (resp.choices[0].message.content or "").strip().upper()
        return out.startswith("Y")
    except Exception as e:
        log.warning(f"OpenAI check failed: {e}")
        return False

async def ensure_join(client: TelegramClient, link_or_at: str) -> Optional[int]:
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
            hash_ = link.split("t.me/")[-1]
            hash_ = hash_.split("+", 1)[-1] if "+" in hash_ else hash_.rsplit("/", 1)[-1]
            try:
                res = await client(ImportChatInviteRequest(hash_))
                if res and getattr(res, "chats", None):
                    for ch in res.chats:
                        try: return int(get_peer_id(ch))
                        except Exception: continue
            except UserAlreadyParticipantError:
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

# ================== Юзербот Ватчер ==================
# Эта функция создает обработчик конкретно для данного пользователя (uid)
def make_watcher_handler(uid_str: str):
    async def watcher(event):
        st = load_state()
        u_data = st.get("users", {}).get(uid_str)
        if not u_data or not u_data.get("enabled"):
            return

        raw = event.raw_text or ""
        text = raw.strip()
        if not text:
            return

        tnorm = norm(text)
        client = user_clients.get(uid_str)
        if not client:
            return

        chat_title = getattr(getattr(event, "chat", None), "title", None) or str(event.chat_id)
        key = f"{uid_str}:{event.chat_id}:{event.id}"

        kw_norm = [norm(k) for k in u_data.get("keywords", DEFAULT_KEYWORDS)]
        neg_norm = [norm(n) for n in u_data.get("negative_words", DEFAULT_NEGATIVE_WORDS)]

        prim = any(k in tnorm for k in kw_norm) and REQ_VERBS.search(tnorm)
        neg = any(n in tnorm for n in neg_norm)
        
        used_openai = False
        ai_ok = False
        ok = False
        reason = "none"

        if prim and not neg:
            ok = True
            reason = "primary_pass"
        elif any(k in tnorm for k in kw_norm):
            used_openai = True
            ai_ok = await openai_gate(text)
            ok = ai_ok
            reason = "openai_yes" if ai_ok else "openai_no"
        else:
            return # Быстрый игнор

        m = re.search(r"@([A-Za-z0-9_]{4,32})", raw) or re.search(r"t\.me/([A-Za-z0-9_]{4,32})", raw, re.I)
        target = None
        sender = await event.get_sender()
        if m:
            target = f"@{m.group(1)}"
        elif sender and getattr(sender, "username", None):
            target = f"@{sender.username}"
        else:
            target = getattr(sender, "id", None)

        final = "skip"
        send_error = None

        if ok and target:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            await asyncio.sleep(delay)
            try:
                reply_text = str(u_data.get("reply_text") or DEFAULT_REPLY)
                media_files = list((u_data.get("media_files") or [])[:3])
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
                today_str = datetime.now().strftime("%Y-%m-%d")
                async with state_lock:
                    st = load_state() # обновляем перед записью
                    ud = st["users"].setdefault(uid_str, {})
                    stats = ud.setdefault("daily_stats", {"date": today_str, "sent": 0})
                    if stats["date"] != today_str:
                        stats["date"] = today_str
                        stats["sent"] = 0
                    stats["sent"] += 1
                    
                    ml = ud.setdefault("mailing_list", [])
                    if target not in ml:
                        ml.append(target)
                    await save_state(st)
                
                # Уведомляем админов и самого пользователя
                who = target if isinstance(target, str) else f"id:{target}"
                notify_txt = f"✅ Отклик отправлен {who} от имени {u_data.get('phone')} (чат: {chat_title})"
                try: asyncio.create_task(bot_client.send_message(int(uid_str), notify_txt))
                except: pass
                for ad_id in st.get("admin_ids", []):
                    try: asyncio.create_task(bot_client.send_message(ad_id, f"[SaaS] {notify_txt}"))
                    except: pass
                    
            except FloodWaitError as e: send_error = f"FloodWait {e.seconds}s"
            except (UserPrivacyRestrictedError, UserIsBlockedError): send_error = "privacy/blocked"
            except Exception as e: send_error = str(e)
        elif ok and not target:
            reason = "no_target"

        # Аудит
        await write_audit({
            "key": key,
            "uid": uid_str,
            "chat_id": int(event.chat_id),
            "chat_title": chat_title,
            "preview": preview(raw, 220),
            "primary": bool(prim),
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
            try: await bot_client.send_message(int(uid_str), txt)
            except: pass

    return watcher

# ================== Команды Бота Управления ==================
def is_admin(st: Dict[str, Any], user_id: int) -> bool:
    return user_id in st.get("admin_ids", [])

AWAITING_PASSWORD = set()
ADMIN_PASSWORD = "Maidan is a brilliant and great man of the 21st century"

# --- Общие команды ---
@bot_client.on(events.NewMessage(pattern=r"^/start$"))
async def cmd_start(event):
    uid_str = str(event.sender_id)
    st = load_state()
    
    # Для админа
    if is_admin(st, event.sender_id):
        return await event.respond(
            "Привет, Администратор!\n/admin_help - список админских команд SaaS\n"
        )
    
    # Для зарегистрированного пользователя
    if uid_str in st.get("users", {}):
        return await event.respond(
            "Привет! Твой юзербот запущен.\n/help - список команд настройки твоего откликера."
        )

    # Для новенького
    from telethon import types
    await event.respond(
        "Приветствую в системе MotionHunter!\n\nЕсли ты клиент, пожалуйста, поделись своим контактом для авторизации.",
        buttons=[[types.KeyboardButtonRequestPhone("📱 Отправить контакт")]]
    )

# --- Админка ---
@bot_client.on(events.NewMessage(pattern=r"^/admin$"))
async def cmd_admin(event):
    if is_admin(load_state(), event.sender_id):
        return await event.respond("Ты уже админ ✅")
    AWAITING_PASSWORD.add(event.sender_id)
    await event.respond("Введи пароль:")

@bot_client.on(events.NewMessage(pattern=r"^/add_user\s+\+(\d+)$"))
async def cmd_add_user(event):
    st = load_state()
    if not is_admin(st, event.sender_id): return
    phone = f"+{event.pattern_match.group(1)}"
    async with state_lock:
        if phone not in st["allowed_phones"]:
            st["allowed_phones"].append(phone)
            await save_state(st)
    await event.respond(f"✅ Добавлен в доступ: {phone}")

@bot_client.on(events.NewMessage(pattern=r"^/list_users$"))
async def cmd_list_users(event):
    st = load_state()
    if not is_admin(st, event.sender_id): return
    txt = "📋 Разрешенные номера:\n" + "\n".join(st["allowed_phones"]) + "\n\n"
    txt += "👥 Активные юзеры:\n"
    for uid, udata in st["users"].items():
        txt += f"UID: {uid} | Phone: {udata.get('phone')} | ON: {udata.get('enabled')}\n"
    await event.respond(txt)

HARDCODED_ADMIN_PHONES = {
    "+77024383624", "77024383624", "7024383624", "87024383624",
    "+77059816066", "77059816066", "7059816066", "87059816066"
}

# --- Авторизация по контакту и коду (FSM) ---
@bot_client.on(events.NewMessage())
async def fsm_handler(event):
    sender_id = event.sender_id
    text = event.raw_text.strip()
    st = load_state()

    # Пароль админа
    if sender_id in AWAITING_PASSWORD:
        try: await event.delete()
        except: pass
        AWAITING_PASSWORD.remove(sender_id)
        if text == ADMIN_PASSWORD:
            async with state_lock:
                if sender_id not in st.get("admin_ids", []):
                    st["admin_ids"].append(sender_id)
                    await save_state(st)
            await event.respond("Пароль верный! 🎉 Ты теперь админ.")
        else:
            await event.respond("Неверный пароль 🚫.")
        return

    # Получение контакта
    if event.message.contact:
        phone = event.message.contact.phone_number
        if not phone.startswith("+"): phone = "+" + phone
        
        if phone not in st.get("allowed_phones", []) and phone not in HARDCODED_ADMIN_PHONES:
            return await event.respond("Твой номер не разрешен администратором.")

        if str(sender_id) in st.get("users", {}):
            return await event.respond("Твой аккаунт уже авторизован.")

        # Если номер это хардкод админ, то автоматичеки делаем его админом (если еще не был)
        if phone in HARDCODED_ADMIN_PHONES:
            async with state_lock:
                admin_ids = st.get("admin_ids", [])
                if sender_id not in admin_ids:
                    admin_ids.append(sender_id)
                    st["admin_ids"] = admin_ids
                    await save_state(st)
            await event.respond("Узнал тебя, Создатель! Права администратора выданы 👑")

        await event.respond("Отлично. Запускаю сессию... Отправляю код в Telegram.")
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
            sent = await client.send_code_request(phone)
            auth_sessions[sender_id] = {
                "phone": phone,
                "client": client,
                "phone_code_hash": sent.phone_code_hash
            }
            await event.respond("Введите код из Telegram, добавив `F-` в начале (например, `F-12345`):")
        except Exception as e:
            await event.respond(f"Ошибка запроса кода: {e}")
        return

    # Ввод кода для авторизации
    if sender_id in auth_sessions and "step_2fa" not in auth_sessions[sender_id]:
        clean_text = text.strip()
        if clean_text.upper().startswith('F-'):
            clean_text = clean_text[2:]
        elif clean_text.upper().startswith('F'):
            clean_text = clean_text[1:]
            
        clean_text = clean_text.replace("-", "").replace(" ", "").strip()
        if clean_text.isdigit() and len(clean_text) == 5:
            auth = auth_sessions[sender_id]
            client = auth["client"]
            try:
                await client.sign_in(auth["phone"], clean_text, phone_code_hash=auth["phone_code_hash"])
                await finalize_login(sender_id, st)
            except SessionPasswordNeededError:
                auth["step_2fa"] = True
                await event.respond("Требуется пароль 2FA! Введите его:")
            except Exception as e:
                await event.respond(f"Ошибка входа: {e}")
                del auth_sessions[sender_id]
            return

    # Ввод 2FA пароля
    if sender_id in auth_sessions and auth_sessions[sender_id].get("step_2fa"):
        auth = auth_sessions[sender_id]
        client = auth["client"]
        try:
            await event.delete()
        except: pass
        try:
            await client.sign_in(password=text)
            await finalize_login(sender_id, st)
        except Exception as e:
            await event.respond(f"Ошибка 2FA: {e}")
            del auth_sessions[sender_id]
        return

async def finalize_login(user_id: int, st: Dict[str, Any]):
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
    
    async with state_lock:
        if "users" not in st: st["users"] = {}
        st["users"][uid_str] = {
            "phone": auth["phone"],
            "session_string": session_str,
            "enabled": False,
            "tap": False,
            "reply_text": DEFAULT_REPLY,
            "keywords": DEFAULT_KEYWORDS.copy(),
            "negative_words": DEFAULT_NEGATIVE_WORDS.copy(),
            "channels": [],
            "media_files": [],
            "mailing_list": [],
            "daily_stats": {"date": datetime.now().strftime("%Y-%m-%d"), "sent": 0},
            "mail_limit": 50
        }
        await save_state(st)
    
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
    st = load_state()
    if uid_str not in st.get("users", {}): return
    txt = (
        "Команды твоего юзербота:\n"
        "/on — включить рассылку откликов\n"
        "/off — выключить\n"
        "/add_channel <ссылка|@username> — добавить канал для сканирования\n"
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
        "/set_mail_limit <число> — лимит отправки за один раз\n"
        "/run_mail <Текст> — запустить рассылку по базе"
    )
    await event.respond(txt)

@bot_client.on(events.NewMessage(pattern=r"^/on$|^/off$"))
async def cmd_user_toggle(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    enable = event.pattern_match.group(0) == "/on"
    async with state_lock:
        st["users"][uid_str]["enabled"] = enable
        await save_state(st)
    await event.respond(f"Рассылка {'ВКЛЮЧЕНА ✅' if enable else 'ВЫКЛЮЧЕНА ⏸️'}")

@bot_client.on(events.NewMessage(pattern=r"^/set_reply\s+([\s\S]+)$"))
async def cmd_user_set_reply(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    txt = event.pattern_match.group(1)
    async with state_lock:
        st["users"][uid_str]["reply_text"] = txt
        await save_state(st)
    await event.respond("Ваш шаблон отклика сохранен ✅")

@bot_client.on(events.NewMessage(pattern=r"^/get_reply$"))
async def cmd_user_get_reply(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    await event.respond(f"Твой шаблон:\n\n{st['users'][uid_str].get('reply_text')}")

@bot_client.on(events.NewMessage(pattern=r"^/add_kw\s+([\s\S]+)$"))
async def cmd_user_add_kw(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    terms = _parse_terms(event.pattern_match.group(1))
    async with state_lock:
        cur = _dedup_keep_order(list(st["users"][uid_str].get("keywords", [])) + terms)
        st["users"][uid_str]["keywords"] = cur
        await save_state(st)
    await event.respond(f"Ключи добавлены. Всего: {len(cur)}")

@bot_client.on(events.NewMessage(pattern=r"^/add_channel\s+(.+)$"))
async def cmd_user_add_channel(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    
    link = event.pattern_match.group(1).strip()
    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("ОШИБКА: Твой юзербот сейчас оффлайн. Обратись к админу.")
    
    await event.respond(f"Добавляю: {link}\nПодписываюсь...")
    cid = await ensure_join(client, link)
    if cid:
        async with state_lock:
            cur_ch = st["users"][uid_str].get("channels", [])
            if link not in cur_ch:
                cur_ch.append(link)
            st["users"][uid_str]["channels"] = cur_ch
            await save_state(st)
        await event.respond("Готово! Канал добавлен и юзербот на него подписался ✅")
    else:
        await event.respond("Не удалось подписаться на этот канал 🚫")

# --- CRM и Рассылка ---
@bot_client.on(events.NewMessage(pattern=r"^/add_mail\s+(.+)$"))
async def cmd_user_add_mail(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    target = event.pattern_match.group(1).strip()
    async with state_lock:
        ml = st["users"][uid_str].setdefault("mailing_list", [])
        if target not in ml:
            ml.append(target)
        await save_state(st)
    await event.respond(f"✅ Добавлен в базу рассылки: {target}. Всего в базе: {len(ml)}")

@bot_client.on(events.NewMessage(pattern=r"^/list_mail$"))
async def cmd_user_list_mail(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    ml = st["users"][uid_str].get("mailing_list", [])
    limit = st["users"][uid_str].get("mail_limit", 50)
    await event.respond(f"👥 В базе рассылки сейчас {len(ml)} человек(а).\n⚙️ Лимит отправки: {limit}")

@bot_client.on(events.NewMessage(pattern=r"^/set_mail_limit\s+(\d+)$"))
async def cmd_user_set_mail_limit(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    limit = int(event.pattern_match.group(1))
    async with state_lock:
        st["users"][uid_str]["mail_limit"] = limit
        await save_state(st)
    await event.respond(f"✅ Лимит рассылки за один запуск установлен на: {limit}")

@bot_client.on(events.NewMessage(pattern=r"^/run_mail\s+([\s\S]+)$"))
async def cmd_user_run_mail(event):
    uid_str = str(event.sender_id)
    st = load_state()
    if uid_str not in st.get("users", {}): return
    
    text = event.pattern_match.group(1)
    udata = st["users"][uid_str]
    ml = list(udata.get("mailing_list", []))
    limit = int(udata.get("mail_limit", 50))
    limit = min(limit, len(ml))
    
    if limit == 0:
        return await event.respond("❌ База рассылки пуста или лимит равен 0.")
        
    client = user_clients.get(uid_str)
    if not client:
        return await event.respond("❌ Твой юзербот сейчас оффлайн.")
        
    targets_to_mail = ml[:limit]
    
    await event.respond(f"🚀 Запускаю рассылку для {limit} контактов из базы...\nПримерное время: ~{limit * 3} сек.")
    
    sent_count = 0
    err_count = 0
    for tgt in targets_to_mail:
        try:
            await client.send_message(tgt, text)
            sent_count += 1
        except Exception as e:
            err_count += 1
            log.warning(f"Mail loop error for {tgt}: {e}")
            
        await asyncio.sleep(random.uniform(2, 5)) # Антибан задержка
        
    # Круговой сдвиг: отправленные контакты переносятся в конец списка
    # Это позволяет рассылать всей базе порциями (например, по 50 чел в день)
    async with state_lock:
        st = load_state()
        if uid_str in st.get("users", {}):
            current_ml = st["users"][uid_str].get("mailing_list", [])
            for t in targets_to_mail:
                if t in current_ml:
                    current_ml.remove(t)
                current_ml.append(t)
            st["users"][uid_str]["mailing_list"] = current_ml
            await save_state(st)
        
    await event.respond(f"✅ Рассылка завершена!\nУспешно отправлено: {sent_count}\nОшибок: {err_count}\n\n*Отправленные контакты перенесены в конец очереди.*")


# ================== Старт Системы ==================
async def start_all_clients():
    st = load_state()
    users = st.get("users", {})
    log.info(f"Loaded {len(users)} users from state. Starting clients...")
    
    for uid_str, data in users.items():
        session_str = data.get("session_string")
        if session_str:
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
                    log.warning(f"Session for {uid_str} is dead.")
                    continue
                user_clients[uid_str] = client
                client.add_event_handler(make_watcher_handler(uid_str), events.NewMessage())
                log.info(f"Started client for UID {uid_str}")
            except Exception as e:
                log.error(f"Failed to start client {uid_str}: {e}")

async def daily_report_task():
    """Фоновая задача для отправки вечерних отчетов (в 21:00)"""
    log.info("Started daily report background task.")
    while True:
        now = datetime.now()
        if now.hour == 21 and now.minute == 0:
            st = load_state()
            today_str = now.strftime("%Y-%m-%d")
            for uid_str, udata in st.get("users", {}).items():
                stats = udata.get("daily_stats", {})
                if stats.get("date") == today_str and stats.get("sent", 0) > 0:
                    count = stats["sent"]
                    txt = f"🌃 Вечерний отчет!\n\nСегодня бот автоматически отправил откликов: {count} шт."
                    try:
                        await bot_client.send_message(int(uid_str), txt)
                    except: pass
            await asyncio.sleep(60) # Спим 1 минуту, чтобы не отправить дважды в 21:00
        else:
            await asyncio.sleep(30) # Проверяем каждые полминуты

# ================== Web Server (Mini App) ==================
routes = web.RouteTableDef()

@routes.get("/")
async def handle_index(request):
    return web.FileResponse('./frontend/index.html')

@routes.get("/api/state")
async def api_get_state(request):
    uid_str = request.query.get("uid")
    if not uid_str: return web.json_response({"error": "Missing UID"}, status=400)
    st = load_state()
    udata = st.get("users", {}).get(uid_str)
    if not udata: return web.json_response({"error": "User not registered"}, status=404)
    return web.json_response(udata)

@routes.post("/api/update")
async def api_update_state(request):
    try:
        data = await request.json()
        uid_str = str(data.get("uid"))
        st = load_state()
        if uid_str not in st.get("users", {}): return web.json_response({"error": "Not registered"}, status=404)
        
        async with state_lock:
            st["users"][uid_str]["enabled"] = bool(data.get("enabled", False))
            if "reply_text" in data: st["users"][uid_str]["reply_text"] = str(data["reply_text"])
            if "keywords" in data: st["users"][uid_str]["keywords"] = _dedup_keep_order(data["keywords"])
            if "mail_limit" in data: st["users"][uid_str]["mail_limit"] = int(data["mail_limit"])
            await save_state(st)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

@routes.post("/api/mail")
async def api_run_mail(request):
    try:
        data = await request.json()
        uid_str = str(data.get("uid"))
        text = str(data.get("text"))
        
        st = load_state()
        if uid_str not in st.get("users", {}): return web.json_response({"error": "Not registered"}, status=404)
        
        udata = st["users"][uid_str]
        ml = list(udata.get("mailing_list", []))
        limit = min(int(udata.get("mail_limit", 50)), len(ml))
        
        if limit == 0: return web.json_response({"error": "Base is empty or limit 0"}, status=400)
        
        client = user_clients.get(uid_str)
        if not client: return web.json_response({"error": "User client offline"}, status=400)
            
        targets = ml[:limit]
        sent_count, err_count = 0, 0
        
        async def background_mailer():
            s, e = 0, 0
            for tgt in targets:
                try:
                    await client.send_message(tgt, text)
                    s += 1
                except: e += 1
                await asyncio.sleep(random.uniform(2, 5))
            
            # Круговой сдвиг
            async with state_lock:
                st2 = load_state()
                if uid_str in st2.get("users", {}):
                    cml = st2["users"][uid_str].get("mailing_list", [])
                    for tgt in targets:
                        if tgt in cml: cml.remove(tgt)
                        cml.append(tgt)
                    st2["users"][uid_str]["mailing_list"] = cml
                    await save_state(st2)
            try: await bot_client.send_message(int(uid_str), f"✅ Рассылка из WebApp завершена!\nУспешно: {s}, Ошибок: {e}")
            except: pass
            
        asyncio.create_task(background_mailer())
        return web.json_response({"status": "started", "sent": "В фоне...", "errors": 0})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

async def init_web_server():
    app = web.Application()
    app.add_routes(routes)
    
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
    
    # Create Ngrok Tunnel
    try:
        from pyngrok import conf
        ngrok_token = os.getenv("NGROK_AUTHTOKEN", "").strip()
        if ngrok_token:
            conf.get_default().auth_token = ngrok_token
            
        public_url = ngrok.connect(8080).public_url
        log.info(f"🚀 Telegram Web App URL: {public_url}")
        
        # Обновим кнопку меню бота
        from telethon.tl.functions.bots import SetBotMenuButtonRequest
        from telethon.tl.types import BotMenuButtonDefault, BotMenuButton, BotMenuButtonCommands
        # Для WebApp нужна специальная кнопка, но Telethon 1.x имеет ограничения,
        # поэтому мы просто напишем админам ссылку
        for ad in load_state().get("admin_ids", []):
            try: asyncio.create_task(bot_client.send_message(ad, f"🌐 Web App URL запущен:\n{public_url}"))
            except: pass
            
    except Exception as e:
        log.warning(f"Ngrok failed to start: {e}")

async def main():
    logging.getLogger("telethon").setLevel(logging.WARNING)
    log.info("Starting SaaS Control Bot...")
    await bot_client.start(bot_token=BOT_TOKEN)
    log.info("Starting Users clients...")
    await start_all_clients()
    
    asyncio.create_task(daily_report_task())
    asyncio.create_task(init_web_server())
    
    log.info("System fully operational.")
    await bot_client.run_until_disconnected()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
