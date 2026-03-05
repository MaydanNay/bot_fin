import pexpect
import sys

def setup_tunnel():
    print("Starting SSH tunnel...")
    child = pexpect.spawn('ssh -o StrictHostKeyChecking=no -N -L 3306:127.0.0.1:3306 cr01315@5.23.50.183')
    
    try:
        i = child.expect(['password:', 'yes/no'], timeout=10)
        if i == 1:
            child.sendline('yes')
            child.expect('password:')
        
        child.sendline('UM1qoeYqd3Uj')
        print("Password sent. Tunnel is up!")
        
        # Keep running
        child.interact()
    except Exception as e:
        print("Failed:", str(e))
        
if __name__ == '__main__':
    setup_tunnel()
