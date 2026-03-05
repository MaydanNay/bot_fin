import os
import json
import logging
import asyncio
import aiomysql
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

load_dotenv()

# Logger
log = logging.getLogger("db")
logging.basicConfig(level=logging.INFO)

# Config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

pool: Optional[aiomysql.Pool] = None

async def init_db():
    global pool
    log.info(f"Initializing MySQL pool for {DB_NAME} at {DB_HOST}...")
    pool = await aiomysql.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        autocommit=True,
        charset='utf8mb4'
    )
    
    # Create tables if not exist
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Users
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    uid VARCHAR(64) PRIMARY KEY,
                    phone VARCHAR(32),
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
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # CRM
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_contacts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(64),
                    contact VARCHAR(128),
                    UNIQUE KEY (user_id, contact),
                    FOREIGN KEY (user_id) REFERENCES users(uid) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # Web Tokens
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS web_tokens (
                    token VARCHAR(128) PRIMARY KEY,
                    user_id VARCHAR(64),
                    FOREIGN KEY (user_id) REFERENCES users(uid) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Channels
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    user_id VARCHAR(64),
                    channel_link VARCHAR(255),
                    PRIMARY KEY (user_id, channel_link),
                    FOREIGN KEY (user_id) REFERENCES users(uid) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # Admins (Static list mostly, but let's have a table for flexibility)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    admin_id BIGINT PRIMARY KEY
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # Allowed Phones
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS allowed_phones (
                    phone VARCHAR(32) PRIMARY KEY
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
    log.info("Database schema verified.")

async def close_db():
    if pool:
        pool.close()
        await pool.wait_closed()
        log.info("Database pool closed.")

# --- HELPERS ---

async def get_user(uid: str) -> Optional[Dict[str, Any]]:
    if not pool: return None
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT * FROM users WHERE uid = %s", (uid,))
            user = await cur.fetchone()
            if user:
                # Deserialize lists
                user['keywords'] = json.loads(user['keywords']) if user['keywords'] else []
                user['negative_words'] = json.loads(user['negative_words']) if user['negative_words'] else []
                user['enabled'] = bool(user['enabled'])
                return user
    return None

async def upsert_user(uid: str, data: Dict[str, Any]):
    if not pool: return
    
    kw = json.dumps(data.get("keywords", []))
    neg = json.dumps(data.get("negative_words", []))
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO users (uid, phone, session_string, name, username, enabled, reply_text, keywords, negative_words, mail_limit, daily_sent, daily_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                phone=VALUES(phone), session_string=VALUES(session_string), name=VALUES(name), username=VALUES(username), enabled=VALUES(enabled),
                reply_text=VALUES(reply_text), keywords=VALUES(keywords), negative_words=VALUES(negative_words),
                mail_limit=VALUES(mail_limit), daily_sent=VALUES(daily_sent), daily_date=VALUES(daily_date)
            """, (
                uid, data.get("phone"), data.get("session_string"), data.get("name"), data.get("username"),
                data.get("enabled", False), data.get("reply_text"), kw, neg,
                data.get("mail_limit", 50), data.get("daily_sent", 0), data.get("daily_date")
            ))

async def get_all_uids() -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT uid FROM users")
            rows = await cur.fetchall()
            return [str(r[0]) for r in rows]

async def update_user_field(uid: str, field: str, value: Any):
    if not pool: return
    if field in ["keywords", "negative_words"]:
        value = json.dumps(value)
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"UPDATE users SET {field} = %s WHERE uid = %s", (value, uid))

# --- CRM ---

async def add_crm_contacts(uid: str, contacts: List[Any]):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for c in contacts:
                try:
                    await cur.execute("INSERT IGNORE INTO crm_contacts (user_id, contact) VALUES (%s, %s)", (uid, str(c)))
                except: continue

async def delete_crm_contact(uid: str, contact: Any):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM crm_contacts WHERE user_id = %s AND contact = %s", (uid, str(contact)))

async def get_crm_contacts(uid: str, query: str = "") -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = "SELECT contact FROM crm_contacts WHERE user_id = %s"
            params = [uid]
            if query:
                sql += " AND LOWER(contact) LIKE %s"
                params.append(f"%{query.lower()}%")
            sql += " ORDER BY id ASC"
            await cur.execute(sql, params)
            rows = await cur.fetchall()
            return [str(r[0]) for r in rows]

async def move_to_end(uid: str, contact: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # We move by deleting and re-inserting, or by updating ID if we want to be fancy.
            # Re-inserting is safer for AUTO_INCREMENT if we want them at the end.
            await cur.execute("DELETE FROM crm_contacts WHERE user_id = %s AND contact = %s", (uid, contact))
            await cur.execute("INSERT INTO crm_contacts (user_id, contact) VALUES (%s, %s)", (uid, contact))

async def get_crm_count(uid: str) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM crm_contacts WHERE user_id = %s", (uid,))
            row = await cur.fetchone()
            return row[0] if row else 0

# --- CHANNELS ---

async def add_channel(uid: str, link: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT IGNORE INTO channels (user_id, channel_link) VALUES (%s, %s)", (uid, link))

async def remove_channel(uid: str, link: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM channels WHERE user_id = %s AND channel_link = %s", (uid, link))

async def get_channels(uid: str) -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT channel_link FROM channels WHERE user_id = %s", (uid,))
            rows = await cur.fetchall()
            return [str(r[0]) for r in rows]

# --- TOKENS ---

async def set_web_token(token: str, uid: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO web_tokens (token, user_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)", (token, uid))

async def get_uid_by_token(token: str) -> Optional[str]:
    if not pool: return None
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT user_id FROM web_tokens WHERE token = %s", (token,))
            row = await cur.fetchone()
            return str(row[0]) if row else None

# --- ADMINS ---

async def get_admins() -> List[int]:
    if not pool: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT admin_id FROM admins")
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows]

async def add_admin(admin_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT IGNORE INTO admins (admin_id) VALUES (%s)", (admin_id,))

async def remove_admin(admin_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM admins WHERE admin_id = %s", (admin_id,))

# --- ALLOWED PHONES ---

async def add_allowed_phone(phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT IGNORE INTO allowed_phones (phone) VALUES (%s)", (phone,))

async def remove_allowed_phone(phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM allowed_phones WHERE phone = %s", (phone,))

async def get_allowed_phones() -> List[str]:
    if not pool: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT phone FROM allowed_phones")
            rows = await cur.fetchall()
            return [str(r[0]) for r in rows]

async def is_phone_allowed(phone: str) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM allowed_phones WHERE phone = %s", (phone,))
            return await cur.fetchone() is not None
