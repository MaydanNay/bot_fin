import os
import re
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
    # Маскируем пароль для безопасности
    masked_url = re.sub(r':([^/@]+)@', ':***@', DATABASE_URL)
    log.info(f"Initializing PostgreSQL pool. URL: {masked_url}, DB_NAME: {DB_NAME}")
    try:
        pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=20)
    except Exception as e:
        err_msg = str(e).lower()
        if "does not exist" in err_msg or "database" in err_msg:
            log.warning(f"Database '{DB_NAME}' does not exist. Attempting to create it...")
            try:
                # Build fallback URL to 'postgres' system database
                postgres_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"
                # Connect as superuser to create DB
                temp_conn = await asyncpg.connect(dsn=postgres_url)
                # CREATE DATABASE cannot be run in a transaction
                await temp_conn.execute(f'CREATE DATABASE "{DB_NAME}"')
                await temp_conn.close()
                log.info(f"Database '{DB_NAME}' created successfully.")
                # Try connecting again
                pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=20)
            except Exception as e2:
                log.error(f"Failed to auto-create database '{DB_NAME}': {e2}")
                raise e
        else:
            if "password authentication failed" in err_msg:
                log.error(f"❌ DATABASE AUTH ERROR: Password for user '{DB_USER}' does not match the one in existing Docker Volume.")
                log.error("To fix this, either use the old password in .env or run: docker compose down -v")
            else:
                log.error(f"Failed to connect to PostgreSQL: {e}")
            raise e
        
    async with pool.acquire() as conn:
        # 1. Создание таблиц (если их нет)
        # Users: Phone is PK to support web-registration before Telegram link
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                phone VARCHAR(32) PRIMARY KEY,
                uid VARCHAR(64) UNIQUE
            );
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS crm_contacts (
                id SERIAL PRIMARY KEY,
                uid VARCHAR(64) REFERENCES users(uid) ON DELETE CASCADE,
                contact VARCHAR(128),
                UNIQUE (uid, contact)
            );
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS web_tokens (
                token VARCHAR(128) PRIMARY KEY,
                phone VARCHAR(32) REFERENCES users(phone) ON DELETE CASCADE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                uid VARCHAR(64) REFERENCES users(uid) ON DELETE CASCADE,
                channel_link VARCHAR(255),
                PRIMARY KEY (uid, channel_link)
            );
        """)

        await conn.execute("CREATE TABLE IF NOT EXISTS admins (admin_id BIGINT PRIMARY KEY);")
        await conn.execute("CREATE TABLE IF NOT EXISTS allowed_phones (phone VARCHAR(32) PRIMARY KEY);")
        
        # 2. Migration / Schema Verification
        # Добавляем все недостающие колонки во все таблицы
        migrations = [
            # Таблица users
            ("users", "uid", "VARCHAR(64) UNIQUE"),
            ("users", "password_hash", "TEXT"),
            ("users", "session_string", "TEXT"),
            ("users", "name", "VARCHAR(128)"),
            ("users", "username", "VARCHAR(128)"),
            ("users", "enabled", "BOOLEAN DEFAULT FALSE"),
            ("users", "reply_text", "TEXT"),
            ("users", "keywords", "TEXT"),
            ("users", "negative_words", "TEXT"),
            ("users", "mail_limit", "INT DEFAULT 50"),
            ("users", "daily_sent", "INT DEFAULT 0"),
            ("users", "daily_date", "DATE"),
            ("users", "expires_at", "TIMESTAMP WITH TIME ZONE"),
            
            # Таблица crm_contacts (доп. поля)
            ("crm_contacts", "created_at", "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"),
            ("crm_contacts", "source", "VARCHAR(255)"),
            
            # Таблица channels
            ("channels", "channel_id", "BIGINT"),
            ("channels", "enabled", "BOOLEAN DEFAULT TRUE"),
            ("channels", "type", "VARCHAR(32) DEFAULT 'channel'"),
        ]
        
        for table, column, col_type in migrations:
            try:
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type};")
            except Exception as e:
                log.debug(f"Migration for {table}.{column} skipped: {e}")

    log.info("Database schema verified and migrations applied.")

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
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE uid = $1", uid)
            return await _process_user_row(row)
    except Exception as e:
        log.error(f"Error in get_user({uid}): {e}")
        raise e

async def get_user_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE phone = $1", phone)
        return await _process_user_row(row)

async def register_web_user(phone: str, password_hash: str):
    if not pool: return
    uid = f"web_{phone}"
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (uid, phone, password_hash, keywords, negative_words, daily_date)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (phone) DO NOTHING
        """, uid, phone, password_hash, "[]", "[]", datetime.now().date())

async def admin_add_user(phone: str):
    if not pool: return None
    uid = f"admin_{phone}"
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (uid, phone, keywords, negative_words, daily_date)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (phone) DO NOTHING
        """, uid, phone, "[]", "[]", datetime.now().date())
    return True

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
    if not phone: 
        log.error(f"Cannot upsert user {uid}: phone is missing in data!")
        return # Phone is mandatory PK now
    
    session_str = data.get("session_string")
    log.info(f"Upserting user {uid} (phone: {phone}), session_length: {len(session_str) if session_str else 0}")
    
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

async def add_crm_contacts(uid: str, contacts: List[Any], source: Optional[str] = None) -> int:
    if not pool: return 0
    
    unique_contacts = list(set([str(c).strip() for c in contacts if str(c).strip()]))
    if not unique_contacts: return 0
    
    async with pool.acquire() as conn:
        try:
            # Используем unnest для вставки всех контактов одним запросом
            # source передаем как второе unnest того же размера или просто $3
            status = await conn.execute("""
                INSERT INTO crm_contacts (uid, contact, source)
                SELECT $1, unnest($2::text[]), $3
                ON CONFLICT (uid, contact) DO UPDATE SET source = EXCLUDED.source
            """, uid, unique_contacts, source)
            
            if status and status.startswith("INSERT "):
                return int(status.split()[-1])
            return 0
        except Exception as e:
            log.error(f"Error bulk inserting crm contacts: {e}")
            return 0

async def delete_crm_contact(uid: str, contact: Any):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM crm_contacts WHERE uid = $1 AND contact = $2", uid, str(contact))

async def get_crm_contacts(uid: str, query: str = "") -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as conn:
        if query:
            sql = "SELECT contact, created_at, source FROM crm_contacts WHERE uid = $1 AND LOWER(contact) LIKE $2 ORDER BY id ASC"
            rows = await conn.fetch(sql, uid, f"%{query.lower()}%")
        else:
            sql = "SELECT contact, created_at, source FROM crm_contacts WHERE uid = $1 ORDER BY id ASC"
            rows = await conn.fetch(sql, uid)
        return [{"contact": r['contact'], "created_at": r['created_at'], "source": r['source']} for r in rows]

async def move_to_end(uid: str, contact: str):
    if not pool: return
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                # Получаем текущий источник перед удалением
                old_source = await conn.fetchval("SELECT source FROM crm_contacts WHERE uid = $1 AND contact = $2", uid, contact)
                await conn.execute("DELETE FROM crm_contacts WHERE uid = $1 AND contact = $2", uid, contact)
                await conn.execute("INSERT INTO crm_contacts (uid, contact, source) VALUES ($1, $2, $3)", uid, contact, old_source)
        except Exception as e:
            log.error(f"Error in move_to_end transaction: {e}")

async def get_crm_count(uid: str) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT COUNT(*) FROM crm_contacts WHERE uid = $1", uid)
        return val if val else 0

# --- CHANNELS ---

async def add_channel(uid: str, link: str, channel_id: int = None, ctype: str = "channel"):
    if not pool: return False
    async with pool.acquire() as conn:
        status = await conn.execute("""
            INSERT INTO channels (uid, channel_link, channel_id, enabled, type) 
            VALUES ($1, $2, $3, TRUE, $4) 
            ON CONFLICT (uid, channel_link) DO UPDATE SET 
                channel_id = EXCLUDED.channel_id,
                type = EXCLUDED.type
        """, uid, link, channel_id, ctype)
        # status has format "INSERT 0 1" for new rows
        if status and status.startswith("INSERT "):
            try:
                count = int(status.split()[-1])
                return count > 0
            except: pass
        return False

async def remove_channel(uid: str, link: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM channels WHERE uid = $1 AND channel_link = $2", uid, link)

async def get_channels(uid: str, query: str = "", ctype: Optional[str] = None) -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as conn:
        where_clause = "WHERE uid = $1"
        args = [uid]
        
        if ctype:
            where_clause += " AND type = $2"
            args.append(ctype)
            
        if query:
            param_idx = len(args) + 1
            where_clause += f" AND LOWER(channel_link) LIKE ${param_idx}"
            args.append(f"%{query.lower()}%")
            
        sql = f"SELECT channel_link, channel_id, enabled, type FROM channels {where_clause}"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def toggle_channel(uid: str, link: str, enabled: bool):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE channels SET enabled = $1 WHERE uid = $2 AND channel_link = $3", enabled, uid, link)

async def toggle_all_channels(uid: str, enabled: bool):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE channels SET enabled = $1 WHERE uid = $2", enabled, uid)

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
        val = await conn.fetchval("""
            SELECT u.uid FROM web_tokens t 
            JOIN users u ON t.phone = u.phone
            WHERE t.token = $1 AND t.created_at > NOW() - INTERVAL '30 days'
        """, token)
        return str(val) if val else None

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

async def get_all_users() -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users ORDER BY uid NULLS LAST")
        return [dict(r) for r in rows]

async def update_user_access(phone: str, months: int):
    if not pool: return
    async with pool.acquire() as conn:
        # Check if user has expires_at
        user = await conn.fetchrow("SELECT expires_at FROM users WHERE phone = $1", phone)
        if not user: return
        
        from datetime import timedelta
        current_expiry = user['expires_at']
        now = datetime.now()
        
        # If already expired or never set, start from now
        if not current_expiry or (current_expiry.tzinfo and current_expiry < now.replace(tzinfo=current_expiry.tzinfo)) or (not current_expiry.tzinfo and current_expiry < now):
            start_date = now
        else:
            start_date = current_expiry
            
        # Add months (approx 30 days per month)
        new_expiry = start_date + timedelta(days=30 * months)
        await conn.execute("UPDATE users SET expires_at = $1 WHERE phone = $2", new_expiry, phone)
        return new_expiry
