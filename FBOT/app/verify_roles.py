import asyncio
import os
import sys

# Добавляем путь к приложению
sys.path.append(os.getcwd())

import db

async def test():
    await db.init_db()
    
    test_user_phone = "+79998887766"
    test_admin_phone = "+79995554433"
    
    print(f"Adding regular user {test_user_phone}...")
    await db.admin_add_user(test_user_phone, months=1, is_admin=False)
    
    print(f"Adding admin user {test_admin_phone}...")
    await db.admin_add_user(test_admin_phone, months=0, is_admin=True)
    
    async with db.pool.acquire() as conn:
        users = await conn.fetch("SELECT phone, uid, is_admin FROM users WHERE phone IN ($1, $2)", test_user_phone, test_admin_phone)
        for u in users:
            print(f"User: {u['phone']}, UID: {u['uid']}, Is Admin: {u['is_admin']}")
            
    await db.close_db()

if __name__ == "__main__":
    asyncio.run(test())
