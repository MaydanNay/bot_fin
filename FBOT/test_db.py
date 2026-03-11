import asyncio
from app.db import pool, get_user, init_db, close_db

async def main():
    await init_db()
    users = await pool.fetch("SELECT uid, system_prompt FROM users LIMIT 1;")
    print("Users:", users)
    await close_db()

if __name__ == "__main__":
    asyncio.run(main())
