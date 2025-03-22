import json
import requests
import game_data_scrapper as game_util
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import statistics as stats
import threading
import binary_search_tree as tree
import os.path




def log(str):
    print(f"[Trend Analyzer] {str}")

# Separates steamids into chunks for parallel processing
def chunk_items(item_list, chunk_size=100):
    for i in range(0, len(item_list), chunk_size):
        yield item_list[i:i+chunk_size]


def fetch_user_game_data(user, api_key):
    url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={user['steamid']}&format=json"
    # print(url)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching a friend's game list: {e}")
        return None
    
    try:
        if response.json().get('response') != None:
            user_game_data = {
                'games'         : response.json().get('response', {}).get('games', []),
                'steamid'       : user['steamid'],
                'personaname'   : user['personaname'],
            }
            # print(f"game--------")
            # print(f'games  : {user_game_data['games']}')
            # print(f"steamid: {user_game_data['steamid']}\npersonaname: {user_game_data['personaname']}")
            return user_game_data
        else: 
            return None
    except ValueError:
        print("Invalid JSON response")
        return None
    
def fetch_game_name(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if str(appid) in data and data[str(appid)]["success"]:
            return data[str(appid)]["data"]["name"]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching game name for appid {appid}: {e}")
    return None  


# Flags
PROCESS_FULL = True

# Main User Constants
api_key = ''
initial_steam_id = ''

# Constants
whitelist_file_path = 'data/friend_whitelist.json'
game_name_file_path = '../steam_games_list.json'
group_library_file_path = 'data/group_library.json'
game_rankings_file_path = 'data/game_rankings.json'
hr_in_mins = 60

# game_names = game_util.load_game_names(game_name_file_path)

# Fetch whitelisted friends and full game data
with open(whitelist_file_path, 'r') as file:
    whitelisted_friends = json.load(file)['whitelisted friends']
game_stats_by_user = []
group_library = []
group_users = []
raw_wt_interest = []

# Use ThreadPoolExecutor to handle threading
with ThreadPoolExecutor(max_workers=10) as executor:
    
    # Retrieve all user game data
    for friend in whitelisted_friends:
        game_stats_by_user.append(fetch_user_game_data(friend, api_key))
    
    # user_game_data_futures = [
    #     executor.submit(fetch_user_game_data, chunk, api_key) for chunk in chunk_items(whitelisted_friends, 100)
    # ]
    # for future in concurrent.futures.as_completed(user_game_data_futures):
    #     game_stats_by_user.extend(future.result())   
    
    try:
        # Check for prior progress
        if os.path.isfile(group_library_file_path) and not PROCESS_FULL:
            with open(group_library_file_path, 'r') as f:
                group_library_stats = json.load(f)['group_libary_stats']
                group_library = group_library_stats['group_library']
            
                # Skip fully processed users
                if len(group_library) != 0:
                    if group_library['users'] != 0:
                        last_user_processed = len(group_library['users']) - 1
                        del game_stats_by_user[:last_user_processed]                    
                        
                        
        user_num = len(game_stats_by_user)
        
        # Sort each users' hours into each game 
        # for each user, ...
        for idx, user_lib_stats in enumerate(game_stats_by_user, start=1):
            log(f'Group Library Progress: {idx}/{user_num} users') 
            user_id = user_lib_stats['steamid']
            user_name = user_lib_stats['personaname']
            
            
            # for each game in a user's library, ...
            for game in user_lib_stats['games']:
                # Only record user data if over 2 hrs of playtime...
                if game['playtime_forever'] <= (2 * hr_in_mins):
                    continue
                
                # Find game in group_library_stats
                group_game = None
                for recorded_game in group_library:
                    # print(recorded_game)
                    if recorded_game['appid'] == game['appid']:
                        group_game = recorded_game
                
                # if game not in group_library_stats, add and set it as current game
                if group_game is None:
                    group_library.append({
                            'appid' : game['appid'],
                            'users' : [],
                            }
                        )
                    game_library_len = len(group_library)
                    group_game = group_library[game_library_len - 1]
                    
                
                # append user name, steamid, hours in last 2 weeks, and total hours played
                group_game['users'].append({
                    'personaname'       : user_name,
                    'steamid'           : user_id,
                    'playtime_forever'  : game['playtime_forever'] / hr_in_mins,
                    'playtime_2weeks'   : game['playtime_2weeks'] / hr_in_mins if game.get('playtime_2weeks') != None else 0,
                })
            
            # Record processed users 
            group_users.append({
                'personaname'   : user_name,
                'steamid'       : user_id,
            })        
        
    finally:
        if len(group_library) != 0:
            with open(group_library_file_path, 'w') as f:
                json.dump({"group_libary_stats": {
                        'group_library' : group_library,
                        'users'         : group_users,
                    }}, f, indent=2)
            log('Saving group_library')
    
    
    
    # TODO: Get name of each game
    
    game_len = len(group_library)
                                            
    # TODO: Thread stuff here (try maps?)
    # for each game, calc group_stats and find weighted avg/interest ranking
    # (total quantity of hours, average hours, number of friends with hours in)
    for idx, game_stats in enumerate(group_library):
        if idx % 10 == 0: log(f'Library Stats Progress: {idx}/{game_len} games') 
        total_recent_hrs_data   = []
        total_forever_hrs_data  = []
        
        
        
        # Tally each user's stats 
        for user_stats in game_stats['users']:
            
            # Create group_stats if non-existent and fill with curr user's
            total_recent_hrs_data.append( user_stats['playtime_2weeks'])
            total_forever_hrs_data.append(user_stats['playtime_forever'])
            
            if game_stats.get('group_stats') == None:
                game_stats['group_stats']   = {
                    'num_interested_friends' : 1,
                    'total_recent_hrs'       : user_stats['playtime_2weeks'],
                    'total_forever_hrs'      : user_stats['playtime_forever'],
                }
                
            # Increment group stats w/ curr user's
            else:
                game_stats['group_stats']['num_interested_friends'] += 1
                game_stats['group_stats']['total_recent_hrs']       += user_stats['playtime_2weeks']
                game_stats['group_stats']['total_forever_hrs']      += user_stats['playtime_forever']
        
        # Special: Don't record games of only one person
        if game_stats['group_stats']['num_interested_friends'] < 2: continue 

        # Compute group stats for the game
        group_stats = game_stats['group_stats']
                                     
        # Average hours b/w users
        group_stats['avg_recent_hrs'] = game_stats['group_stats']['total_recent_hrs']  / game_stats['group_stats']['num_interested_friends']
        group_stats['avg_total_hrs']  = game_stats['group_stats']['total_forever_hrs'] / game_stats['group_stats']['num_interested_friends']
        # TODO: Add avg num of similar games (in user library) 
        
        # Stdev b/w users
        if len(total_recent_hrs_data) >= 2:
            group_stats['stdev_recent_hrs'] = stats.stdev(total_recent_hrs_data)
        else: group_stats['stdev_recent_hrs']   = 0
        if len(total_forever_hrs_data) >= 2:
            group_stats['stdev_forever_hrs']= stats.stdev(total_forever_hrs_data)
        else: group_stats['stdev_forever_hrs']  = 0
        
        # Weighted avg ( (.2 + .5 + .3) / num of analysis categories)
        avg_recent_hr_weight            = .20
        avg_total_hr_weight             = .30
        # stdev_recent_hr_weight          = .10
        # stdev_forever_hr_weight         = .05
        num_interested_friends_weight   = .50

        wt_interest_score = (
                group_stats['avg_recent_hrs']           * avg_recent_hr_weight + 
                group_stats['avg_total_hrs']            * avg_total_hr_weight + 
                # group_stats['stdev_recent_hrs']         * stdev_recent_hr_weight +
                # group_stats['stdev_forever_hrs']        * stdev_forever_hr_weight + 
                group_stats['num_interested_friends']   * num_interested_friends_weight
            ) / 4
        raw_wt_interest.append({
                'game'  : game_stats,
                'name'  : fetch_game_name(game_stats['appid']),
                'score' : wt_interest_score,
            })
    

        
### Sort by Scores
wt_interest_ranking = []

# Binary Search Tree Sort
wt_interest_ranking = tree.tree_sort(raw_wt_interest)

for idx, game in enumerate(wt_interest_ranking): print(f"Game[{idx}]:\n    appid = {game['game']['appid']}\n    interest_score = {game['score']}\n\n")

# Dump ranking data
rankings = {
    'wt_interest_ranking' : wt_interest_ranking,
}
with open(game_rankings_file_path, "w") as f:
    json.dump({"rankings": rankings}, f, indent=2)
     
### End of main