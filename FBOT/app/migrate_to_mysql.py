import json
import asyncio
import os
import logging
from db import init_db, close_db, upsert_user, add_crm_contacts, set_web_token, add_admin

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
log = logging.getLogger("migration")

STATE_FILE = "./data/state.json"

async def migrate():
    if not os.path.exists(STATE_FILE):
        log.error(f"State file {STATE_FILE} not found!")
        return

    log.info("Starting migration from JSON to MySQL...")
    
    try:
        await init_db()
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        log.error("Make sure your IP is whitelisted on Timeweb!")
        return

    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)

    # 1. Admins
    admins = state.get("admin_ids", [])
    log.info(f"Migrating {len(admins)} admins...")
    for admin_id in admins:
        await add_admin(admin_id)

    # 1a. Allowed Phones
    allowed = state.get("allowed_phones", [])
    log.info(f"Migrating {len(allowed)} allowed phones...")
    from db import add_allowed_phone
    for phone in allowed:
        await add_allowed_phone(phone)

    # 2. Users & CRM
    users = state.get("users", {})
    log.info(f"Migrating {len(users)} users...")
    
    for uid, udata in users.items():
        log.info(f"  Migrating user {uid} ({udata.get('name', 'Unknown')})...")
        
        # Основные данные
        user_db_data = {
            "phone": udata.get("phone"),
            "session_string": udata.get("session_string"),
            "name": udata.get("name"),
            "username": udata.get("username"),
            "enabled": udata.get("enabled", False),
            "reply_text": udata.get("reply_text"),
            "keywords": udata.get("keywords", []),
            "negative_words": udata.get("negative_words", []),
            "mail_limit": udata.get("mail_limit", 50),
            "daily_sent": udata.get("daily_stats", {}).get("sent", 0),
            "daily_date": udata.get("daily_stats", {}).get("date")
        }
        await upsert_user(uid, user_db_data)
        
        # CRM контакты
        ml = udata.get("mailing_list", [])
        if ml:
            log.info(f"    Adding {len(ml)} CRM contacts...")
            await add_crm_contacts(uid, ml)
            
        # Каналы
        channels = udata.get("channels", [])
        if channels:
            log.info(f"    Adding {len(channels)} channels...")
            from db import add_channel
            for ch in channels:
                await add_channel(uid, ch)

    # 3. Web Tokens
    tokens = state.get("web_tokens", {})
    log.info(f"Migrating {len(tokens)} web tokens...")
    for token, uid in tokens.items():
        await set_web_token(token, uid)

    log.info("Migration completed successfully! 🎉")
    await close_db()

if __name__ == "__main__":
    asyncio.run(migrate())
