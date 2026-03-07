import asyncio
import db

async def test_bulk():
    print("Testing db...")
    await db.init_db()
    
    # Adding fake crm contacts
    test_contacts = ["@test_1", "@test_2", "@test_3", "@test_1"] # 1 duplicate
    await db.add_crm_contacts("+79998887766", test_contacts)
    res = await db.get_crm_contacts("+79998887766")
    print(f"CRM contacts: {res}")
    
    # Cleanup
    for c in res:
        await db.delete_crm_contact("+79998887766", c)
        
    res2 = await db.get_crm_contacts("+79998887766")
    print(f"CRM contacts after cleanup: {res2}")
        
    await db.close_db()

if __name__ == "__main__":
    asyncio.run(test_bulk())
