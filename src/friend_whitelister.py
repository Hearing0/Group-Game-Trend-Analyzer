import json

raw_friends_file_path = 'data/raw_friend_ids.json'
whitelist_file_path = 'data/friend_whitelist.json'

with open(raw_friends_file_path, 'r') as file:
    raw_friends = json.load(file)['friends']
    friend_num = len(raw_friends)
    friend_ct = 0
    with open(whitelist_file_path, 'r') as f:
        friend_whitelist = json.load(f)['whitelisted friends']    
    
    # For every non-whitelisted friend, prompt user to whitelist them
    try: 
        for friend in raw_friends:
            # Check for already whitelisted friends
            isWhitelisted = False
            for w_friend in friend_whitelist:
                if friend['personaname'] == w_friend['personaname']:
                    isWhitelisted = True
            
            # Skip any whitelisted friends
            if isWhitelisted is True:
                print(f"\n\n[Friend Whitelister] Already Whitelisted [{friend_ct}/{friend_num}]   {friend['personaname']}     (aka {friend.get('realname')})")
                print(f"[Friend Whitelister] Skipped!!!")
                continue
            
            # Display current friend
            friend_ct += 1
            print(f"\n\n[Friend Whitelister] Whitelist [{friend_ct}/{friend_num}]   {friend['personaname']}     (aka {friend.get('realname')})?")
            choice = str(input("Enter choice (y/x for 'yes' or leave blank for 'no'): "))
            
            # If accepted, add to whitelist
            if choice.startswith('y') or choice.startswith('x'):
                friend_whitelist.append(friend)
    
    # Write current whitelisted friends (catches any sudden exits or crashes) 
    finally :
        with open(whitelist_file_path, 'w') as file:
            json.dump({"whitelisted friends": friend_whitelist}, file, indent=2)