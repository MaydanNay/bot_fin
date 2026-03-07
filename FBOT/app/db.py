import os
import json
import logging
import asyncio
import asyncpg
from datetime import datetime
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

load_dotenv()

# Logger
log = logging.getLogger("db")
logging.basicConfig(level=logging.INFO)

# Config (Fallback to defaults if not set, useful for docker-compose)
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "motionhunter")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")

DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

pool: Optional[asyncpg.Pool] = None

async def init_db():
    global pool
    log.info(f"Initializing PostgreSQL pool for {DB_NAME} at {DB_HOST}...")
    try:
        pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=20)
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        raise e
        
    async with pool.acquire() as conn:
        # Create tables if not exist
        
        # Users
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                uid VARCHAR(64) UNIQUE,
                phone VARCHAR(32) PRIMARY KEY,
                password_hash TEXT,
                session_string TEXT,
                name VARCHAR(128),
                username VARCHAR(128),
                enabled BOOLEAN DEFAULT FALSE,
                reply_text TEXT,
                keywords TEXT,
                negative_words TEXT,
                mail_limit INT DEFAULT 50,
                daily_sent INT DEFAULT 0,
                daily_date DATE
            );
        """)
        
        # CRM
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS crm_contacts (
                id SERIAL PRIMARY KEY,
                phone VARCHAR(32) REFERENCES users(phone) ON DELETE CASCADE,
                contact VARCHAR(128),
                UNIQUE (phone, contact)
            );
        """)
        
        # Web Tokens
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS web_tokens (
                token VARCHAR(128) PRIMARY KEY,
                phone VARCHAR(32) REFERENCES users(phone) ON DELETE CASCADE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Channels
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                phone VARCHAR(32) REFERENCES users(phone) ON DELETE CASCADE,
                channel_link VARCHAR(255),
                PRIMARY KEY (phone, channel_link)
            );
        """)
        
        # Admins
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                admin_id BIGINT PRIMARY KEY
            );
        """)

        # Allowed Phones
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS allowed_phones (
                phone VARCHAR(32) PRIMARY KEY
            );
        """)
        
    log.info("Database schema verified.")

async def close_db():
    if pool:
        await pool.close()
        log.info("Database pool closed.")

# --- HELPERS ---

async def _process_user_row(user_record):
    if user_record:
        user = dict(user_record)
        # Deserialize lists
        user['keywords'] = json.loads(user['keywords']) if user['keywords'] else []
        user['negative_words'] = json.loads(user['negative_words']) if user['negative_words'] else []
        user['enabled'] = bool(user['enabled'])
        if user.get('daily_date'):
            user['daily_date'] = str(user['daily_date'])
        return user
    return None

async def get_user(uid: str) -> Optional[Dict[str, Any]]:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE uid = $1", uid)
        return await _process_user_row(row)

async def get_user_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE phone = $1", phone)
        return await _process_user_row(row)

async def register_web_user(phone: str, password_hash: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (phone, password_hash, keywords, negative_words, daily_date)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (phone) DO NOTHING
        """, phone, password_hash, "[]", "[]", datetime.now().date())

async def link_telegram_to_phone(phone: str, uid: str, session_str: str, name: str = None, username: str = None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET uid = $1, session_string = $2, name = $3, username = $4
            WHERE phone = $5
        """, uid, session_str, name, username, phone)

async def upsert_user(uid: str, data: Dict[str, Any]):
    if not pool: return
    
    phone = data.get("phone")
    if not phone: return # Phone is mandatory PK now
    
    kw = json.dumps(data.get("keywords", []))
    neg = json.dumps(data.get("negative_words", []))
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (uid, phone, session_string, name, username, enabled, reply_text, keywords, negative_words, mail_limit, daily_sent, daily_date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (phone) DO UPDATE SET
            uid=EXCLUDED.uid, session_string=EXCLUDED.session_string, name=EXCLUDED.name, username=EXCLUDED.username, enabled=EXCLUDED.enabled,
            reply_text=EXCLUDED.reply_text, keywords=EXCLUDED.keywords, negative_words=EXCLUDED.negative_words,
            mail_limit=EXCLUDED.mail_limit, daily_sent=EXCLUDED.daily_sent, daily_date=EXCLUDED.daily_date
        """, 
            uid, phone, data.get("session_string"), data.get("name"), data.get("username"),
            data.get("enabled", False), data.get("reply_text"), kw, neg,
            data.get("mail_limit", 50), data.get("daily_sent", 0), data.get("daily_date")
        )

async def get_all_uids() -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT uid FROM users")
        return [str(r['uid']) for r in rows if r['uid']]

async def get_all_users() -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users")
        return [await _process_user_row(r) for r in rows]

async def update_user_field(uid: str, field: str, value: Any):
    if not pool: return
    if field in ["keywords", "negative_words"]:
        value = json.dumps(value)
    
    async with pool.acquire() as conn:
        # Для безопасности формируем строку столбца (разрешенный список)
        allowed_fields = ["phone", "session_string", "name", "username", "enabled", "reply_text", "keywords", "negative_words", "mail_limit", "daily_sent", "daily_date"]
        if field in allowed_fields:
            await conn.execute(f"UPDATE users SET {field} = $1 WHERE uid = $2", value, uid)

# --- CRM ---

async def add_crm_contacts(phone: str, contacts: List[Any]):
    if not pool: return
    
    unique_contacts = list(set([str(c) for c in contacts]))
    if not unique_contacts: return
    
    async with pool.acquire() as conn:
        try:
            data = [(phone, c) for c in unique_contacts]
            await conn.executemany("INSERT INTO crm_contacts (phone, contact) VALUES ($1, $2) ON CONFLICT (phone, contact) DO NOTHING", data)
        except Exception as e:
            log.error(f"Error bulk inserting crm contacts: {e}")

async def delete_crm_contact(phone: str, contact: Any):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM crm_contacts WHERE phone = $1 AND contact = $2", phone, str(contact))

async def get_crm_contacts(phone: str, query: str = "") -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        if query:
            sql = "SELECT contact FROM crm_contacts WHERE phone = $1 AND LOWER(contact) LIKE $2 ORDER BY id ASC"
            rows = await conn.fetch(sql, phone, f"%{query.lower()}%")
        else:
            sql = "SELECT contact FROM crm_contacts WHERE phone = $1 ORDER BY id ASC"
            rows = await conn.fetch(sql, phone)
        return [str(r['contact']) for r in rows]

async def move_to_end(phone: str, contact: str):
    if not pool: return
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute("DELETE FROM crm_contacts WHERE phone = $1 AND contact = $2", phone, contact)
                await conn.execute("INSERT INTO crm_contacts (phone, contact) VALUES ($1, $2)", phone, contact)
        except Exception as e:
            log.error(f"Error in move_to_end transaction: {e}")

async def get_crm_count(phone: str) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT COUNT(*) FROM crm_contacts WHERE phone = $1", phone)
        return val if val else 0

# --- CHANNELS ---

async def add_channel(phone: str, link: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO channels (phone, channel_link) VALUES ($1, $2) ON CONFLICT (phone, channel_link) DO NOTHING", phone, link)

async def remove_channel(phone: str, link: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM channels WHERE phone = $1 AND channel_link = $2", phone, link)

async def get_channels(phone: str) -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT channel_link FROM channels WHERE phone = $1", phone)
        return [str(r['channel_link']) for r in rows]

# --- TOKENS ---

async def set_web_token(token: str, phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO web_tokens (token, phone) VALUES ($1, $2)
            ON CONFLICT (token) DO UPDATE SET phone = EXCLUDED.phone
        """, token, phone)

async def get_uid_by_token(token: str) -> Optional[str]:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT u.uid FROM web_tokens t 
            JOIN users u ON t.phone = u.phone 
            WHERE t.token = $1 AND t.created_at > NOW() - INTERVAL '30 days'
        """, token)
        return str(row["uid"]) if (row and row["uid"]) else None

async def get_phone_by_token(token: str) -> Optional[str]:
    if not pool: return None
    async with pool.acquire() as conn:
        val = await conn.fetchval("""
            SELECT phone FROM web_tokens 
            WHERE token = $1 AND created_at > NOW() - INTERVAL '30 days'
        """, token)
        return str(val) if val else None

# --- ADMINS ---

async def get_admins() -> List[int]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT admin_id FROM admins")
        return [int(r["admin_id"]) for r in rows]

async def add_admin(admin_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO admins (admin_id) VALUES ($1) ON CONFLICT (admin_id) DO NOTHING", admin_id)

async def remove_admin(admin_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE admin_id = $1", admin_id)

# --- ALLOWED PHONES ---

async def add_allowed_phone(phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO allowed_phones (phone) VALUES ($1) ON CONFLICT (phone) DO NOTHING", phone)

async def remove_allowed_phone(phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM allowed_phones WHERE phone = $1", phone)

async def get_allowed_phones() -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT phone FROM allowed_phones")
        return [str(r["phone"]) for r in rows]

async def is_phone_allowed(phone: str) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT 1 FROM allowed_phones WHERE phone = $1", phone)
        return val is not None
