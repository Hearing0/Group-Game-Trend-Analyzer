import json
import requests
import game_data_scrapper as game_util
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


# Separates steamids into chunks for parallel processing
def chunk_items(item_list, chunk_size=100):
    for i in range(0, len(item_list), chunk_size):
        yield item_list[i:i+chunk_size]


def fetch_user_game_data(user_id, api_key):
    url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={user_id}&format=json"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching a friend's game list: {e}")
        return None
    
    try:
        if 'games' in response.json():
            user_game_data = response.json().get('response', {}).get('games', [])
            user_game_data['steamid'] = user_id
            return user_game_data
        else: 
            return None
    except ValueError:
        print("Invalid JSON response")
        return None
    


# Main User Constants
api_key = ''
initial_steam_id = ''

# Constants
whitelist_file_path = 'data/friend_whitelist.json'
game_name_file_path = '../steam_games_list.json'

# game_names = game_util.load_game_names(game_name_file_path)

# Fetch whitelisted friends and full game data
with open(whitelist_file_path, 'r') as file:
    whitelisted_friends = json.load(file)['whitelisted friends']['steamid']
game_stats_by_user = []
group_library_stats = []

# Use ThreadPoolExecutor to handle threading
with ThreadPoolExecutor(max_workers=10) as executor:
    
    # Retrieve all user game data
    user_game_data_futures = [
        executor.submit(fetch_user_game_data, chunk, api_key) for chunk in chunk_items(whitelisted_friends, 100)
    ]
    for future in concurrent.futures.as_completed(user_game_data_futures):
        game_stats_by_user.extend(future.result())
        
    
    # Sort each users' hours into each game 
    # for each user, 
    for user_library_stats in game_stats_by_user:
        user_id = user_library_stats['steamid']
        
        # for each game in a user's library, 
        for game in user_library_stats['games']:
            # Find game in group_library_stats
            for recorded_game in group_library_stats:
                if recorded_game['appid'] == game['appid']:
                    group_game = recorded_game
            
            # if game not in group_library_stats, add it
            if group_game is None:
                group_library_stats.append({
                    'game_data': {
                        'appid': game['appid'],
                        'users': [],
                        }
                    })
            
            # append user steamid, hours in last 2 weeks, and total hours played
            group_game['game_data']['users'].append({
                'steamid': user_id,
                # TODO: add the rest of this!!!
            })
            
            
             

    # for each game, sort data by weighted avg (1-10)
    # (total quantity of hours, average hours, number of friends with hours in)
    for wl_friend in whitelisted_friends:
        
        # Calc total quantity of hours

        
        # Average hours b/w users
        
        
        # Num of friends w/ 2+ hrs in
        
        
        # Weighted avg ( (.2 + .5 + .3) / num of analysis categories)
        
        
        # Binary Search Tree Sort
        
    
