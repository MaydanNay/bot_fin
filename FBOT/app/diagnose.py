
import json
import os

STATE_FILE = "./data/state.json"

def main():
    if not os.path.exists(STATE_FILE):
        print("State file not found!")
        return
    
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    
    print(f"Admins: {state.get('admin_ids')}")
    users = state.get("users", {})
    print(f"Total users in state: {len(users)}")
    
    for uid, data in users.items():
        print(f"--- User {uid} ---")
        print(f"Phone: {data.get('phone')}")
        print(f"Name: {data.get('name')}")
        print(f"Username: {data.get('username')}")
        print(f"Session: {'PRESENT' if data.get('session_string') else 'MISSING'}")
        print(f"Enabled: {data.get('enabled')}")

if __name__ == "__main__":
    main()
