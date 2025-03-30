import json
from types import NoneType
import requests
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import statistics as stats
import threading
import binary_search_tree as tree
import os.path
from collections import deque
from datetime import datetime
import time
import traceback
import queue
import numpy as np
from math import log


# Flags
PROCESS_FULL = False
SKIP_GAME_STATS = True

# Main User Constants
api_key = ''
steam_id = ''

# Constants
whitelist_file_path = 'data/friend_whitelist.json'
game_name_file_path = '../steam_games_list.json'
group_library_file_path = 'data/group_library.json'
wt_avg_game_rankings_file_path = 'data/game_rankings.wt_avg.json'
cluster_game_rankings_file_path = 'data/game_rankings.cluster.json'
hr_in_mins = 60
q = queue.Queue()


def print_log(*args):
    print(f"[Trend Analyzer] [{str(datetime.now())[:-3]}] ", end="")
    print(*args)

def p_log(str):
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
        # print(f"Error fetching game name for appid {appid}: {e}")
        return e
    return None  

def fetch_full_game_data(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if str(appid) in data and data[str(appid)]["success"]:
            return data[str(appid)]["data"]
    except requests.exceptions.RequestException as e:
        # print(f"Error fetching game data for appid {appid}: {e}")
        return e
    return None

def process_group_stats(game_stats, game_data):
    # Special: Don't record games of only one person
    if game_stats['group_stats']['num_interested_friends'] < 2: return 
                                    
                                    
    # Compute group stats for the game
    group_stats = game_stats['group_stats']

    # Average hours b/w users
    group_stats['avg_recent_hrs'] = game_stats['group_stats']['total_recent_hrs']  / game_stats['group_stats']['num_interested_friends']
    group_stats['avg_total_hrs']  = game_stats['group_stats']['total_forever_hrs'] / game_stats['group_stats']['num_interested_friends']
    
    # Create list of users' recent hr
    users_recent_hrs    = []
    users_total_hrs     = []
    for userdata in game_stats['users']:
        users_recent_hrs.append(userdata['playtime_2weeks'])
        users_total_hrs.append(userdata['playtime_forever'])
    
    # Stdev b/w users
    if game_stats['group_stats']['num_interested_friends'] >= 2:
        group_stats['stdev_recent_hrs'] = stats.stdev(users_recent_hrs)
        group_stats['stdev_forever_hrs']= stats.stdev(users_total_hrs)
    else: 
        group_stats['stdev_recent_hrs']   = 0
        group_stats['stdev_forever_hrs']  = 0
    
    # Compute inverted stdev weights
    if group_stats['stdev_recent_hrs'] != 0 and group_stats['stdev_recent_hrs'] < 2:
        group_stats['inv_stdev_recent_hrs'] = 1 / group_stats['stdev_recent_hrs']
    else: group_stats['inv_stdev_recent_hrs'] = 0
    if group_stats['stdev_forever_hrs'] != 0 and group_stats['stdev_forever_hrs'] < 2:
        group_stats['inv_stdev_forever_hrs'] = 1 / group_stats['stdev_forever_hrs']
    else: group_stats['inv_stdev_forever_hrs'] = 0
    
    
    # Weighted avg ( (.2 + .5 + .3) / num of analysis categories)
    avg_recent_hr_weight            = .15
    avg_total_hr_weight             = .10
    stdev_recent_hr_weight          = .30
    stdev_forever_hr_weight         = .25
    num_interested_friends_weight   = .20

    wt_interest_score = (
            group_stats['avg_recent_hrs']           * avg_recent_hr_weight + 
            group_stats['avg_total_hrs']            * avg_total_hr_weight + 
            group_stats['inv_stdev_recent_hrs']     * stdev_recent_hr_weight +
            group_stats['inv_stdev_forever_hrs']    * stdev_forever_hr_weight +
            group_stats['num_interested_friends']   * num_interested_friends_weight  
        ) / 4
    game_rank_stats = {
            'game'  : game_stats,
            'game_data' : game_data,
            'name'  : game_data['name'] if game_data != None else 'Unknown',
            'score' : wt_interest_score,
        }
    
    return game_rank_stats

# Workers process all games in the group library
def worker(id: int, app_remaining_q: list, results: list, defunct_game_lib: list, total_games: int):
    global delays_cnt
    delays_cnt = 0
        
    while len(app_remaining_q) > 0:
        # Print current progress
        if len(app_remaining_q) % 50 == 0: 
            print_log(f"Worker {id} Progress: {total_games - len(app_remaining_q)}/{total_games} games")
        
        game = app_remaining_q.pop()
        try: 
            
            # if less than 2 friends interested, skip
            if game['group_stats']['num_interested_friends'] <= 2:
                defunct_game_lib.append(game)
                continue
            
            
            appid = game['appid']
            appdetails_req = fetch_full_game_data(appid)
            
            # if successful, process the game data and put it to the results queue
            if isinstance(appdetails_req, dict):
                delays_cnt = 0
                processed_game = process_group_stats(game, appdetails_req)
                if processed_game != None:
                    results.append(processed_game)
                else: 
                    print_log(f"Game ID {appid} < 2 friends interested. Attempting later...")
                    continue
            
            # If game not found, put it back to the queue and sleep for 5 sec
            elif isinstance(appdetails_req, NoneType):
                print_log(f"Game ID {appid} not found. Attempting later. Sleep for 5 sec...")
                defunct_game_lib.append(game)
                if delays_cnt < 3:
                    delays_cnt += 1
                    time.sleep(5)
                else:
                    print_log(f"Game ID {appid} not found. Too many requests. Sleep for 1 min...")
                    delays_cnt += 1
                    time.sleep(1 * 60)
                continue

            elif appdetails_req.response.status_code == 429:
                app_remaining_q.append(game)
                if delays_cnt < 3:
                    print_log(f'Too many requests. Put App ID {appid} back to deque. Sleep for 30 sec...')
                    delays_cnt += 1
                    time.sleep(30)
                else:
                    print_log(f'Too many requests. Put App ID {appid} back to deque. Sleep for 5 min...')
                    delays_cnt += 1
                    time.sleep(5 * 60)
                continue

            elif appdetails_req.response.status_code == 403:
                print_log(f'Forbidden to access. Put App ID {appid} back to deque. Sleep for 5 min...')
                app_remaining_q.append(game)
                time.sleep(5 * 60)
                continue

            else:
                print_log("ERROR: status code:", appdetails_req.response.status_code)
                print_log(f"Error in App Id: {appid}. Attempting later...")
                app_remaining_q.append(game)
                continue
        except:
            print_log(f"Error in decoding app details request. App id: {appid}")
            traceback.print_exc(limit=5)
            defunct_game_lib.append(game)
            continue

    print(f"worker {id} exiting")
    
def calc_adaptive_stdev(median_hrs):
    # Linear scaling: 5hrs at 10hrs → 50hrs at 100hrs
    slope = (50 - 2) / (100 - 10)  # 0.5
    intercept = 2 - (slope * 10)    # 0.0
    adaptive_stdev = (slope * median_hrs) + intercept
    
    # Clamp to ensure minimum 5hrs and maximum 50hrs
    return max(2, min(250, adaptive_stdev))
    
# Update the game ranking list with the new game data
def append_cluster_ranking(game):
    game_user_data = game['game']
    
    # Adaptive stdev: Wider tolerance for higher playtimes
    playtimes = np.array([u['playtime_forever'] for u in game_user_data['users']])
    median_hrs = np.median(playtimes)
    adaptive_stdev = calc_adaptive_stdev(median_hrs)
    
    # Find clusters: Users within median ± adaptive_stdev
    cluster_size = 0
    for i in range(len(playtimes)):
        if np.abs(playtimes[i] - median_hrs) <= adaptive_stdev: cluster_size += 1
    
    # Cluster bonus (if users form a cohesive group)
    if cluster_size >= 3:
        print(f"cohesion bonus: {log(cluster_size)}")
        cohesion_bonus = log(cluster_size) 
    else: cohesion_bonus = .1
    
    # Base score components
    median_score = median_hrs
    recent_median_score = np.median([u['playtime_2weeks'] for u in game_user_data['users']])
    participation_ratio = len(playtimes) / game_user_data['group_stats']['num_interested_friends']
    
    # Final score calculation
    print(f"median_hrs: {median_hrs}, cluster_size: {cluster_size}, adaptive_stdev: {adaptive_stdev}, participation_ratio: {participation_ratio}\n")
    print(f"median_score: {median_score}, recent_median_score: {recent_median_score}, cohesion_bonus: {cohesion_bonus}\n")
    
    score = (median_score * participation_ratio) * cohesion_bonus + 0.1 * recent_median_score
    
    game['cluster_data'] ={
        'median_hrs': median_hrs,
        'cluster_size': cluster_size,
        'adaptive_stdev': adaptive_stdev,
        'participation_ratio': participation_ratio
    }
    game['cluster_score'] = score
    
    return

### Main Program

# Load API key and initial steam id
api_key = ''
steam_id = ''
with open('main_user_credentials.json', 'r') as file:
    credentials = json.load(file)['user']
    api_key = credentials['api_key']
    steam_id = credentials['steam_id']
    
    
# Fetch whitelisted friends and full game data
with open(whitelist_file_path, 'r') as file:
    whitelisted_friends = json.load(file)['whitelisted friends']
game_stats_by_user = []
group_library = []
group_users = []
raw_wt_interest = []

                
# Retrieve all user game data
for friend in whitelisted_friends:
    game_stats_by_user.append(fetch_user_game_data(friend, api_key))
    

try:
    # Check for prior progress
    if os.path.isfile(group_library_file_path) and not PROCESS_FULL:
        with open(group_library_file_path, 'r') as f:
            group_library_stats = json.load(f)['group_libary_stats']
            group_library = group_library_stats['group_library']
        
            # Skip fully processed users
            if len(group_library) != 0:
                if group_library_stats['users'] != 0:
                    last_processed = len(group_library_stats['users']) - 1
                    del game_stats_by_user[:last_processed]       
                    p_log(f"Continuing from {last_processed} users")
    
    user_num = len(game_stats_by_user)
    
    # Sort each users' hours into each game 
    # for each user, ...
    for idx, user_lib_stats in enumerate(game_stats_by_user, start=1):
        p_log(f'Group Library Progress: {idx}/{user_num} users') 
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
        p_log('Saving group_library')



# for each game, calc group_stats and find weighted avg/interest ranking
# (total quantity of hours, average hours, number of friends with hours in)
game_len = len(group_library)
for idx, game_stats in enumerate(group_library):
    if idx % 10 == 0: p_log(f'Group Library Tally Progress: {idx}/{game_len} games') 
    total_recent_hrs_data   = []
    total_forever_hrs_data  = []
    
    
    game_stats['group_stats']   = {
        'num_interested_friends' : 0,
        'total_recent_hrs'       : 0,
        'total_forever_hrs'      : 0,
    }
    
    # Tally each user's stats 
    for user_stats in game_stats['users']:
        
        # Create group_stats if non-existent and fill with curr user's
        total_recent_hrs_data.append( user_stats['playtime_2weeks'])
        total_forever_hrs_data.append(user_stats['playtime_forever'])
        
        # Increment group stats w/ curr user's stats
        game_stats['group_stats']['num_interested_friends'] += 1
        game_stats['group_stats']['total_recent_hrs']       += user_stats['playtime_2weeks']
        game_stats['group_stats']['total_forever_hrs']      += user_stats['playtime_forever']


## Multi-threaded processing of game data
# Prepare multi-threaded processing
thread_num = 10
workers = []
processed_game_lib = []
error_game_lib = []
group_library_num = len(group_library)

prior_ranking = []
wt_interest_ranking = []



# Check for prior progress
if os.path.isfile('data/game_rankings.json') and not PROCESS_FULL:
    with open('data/game_rankings.json', 'r') as f:
        print_log("Loading prior game rankings...")
        prior_ranking = json.load(f)['rankings']['wt_interest_ranking']
    
        # Skip to last processed games
        if prior_ranking != None:
            print_log("Removing processed games from group_library...")
            removed_games = 0
            for game in prior_ranking:
                for g in group_library:
                    if game['game']['appid'] == g['appid']:
                        removed_games += 1
                        group_library.remove(g)
            
            # Remove processed games from group_library
            p_log(f"Continuing from {removed_games}/{group_library_num} games")
        
        if SKIP_GAME_STATS:
            processed_game_lib = prior_ranking
            group_library_num = len(group_library)



if SKIP_GAME_STATS:
    print_log("Skipping game stats processing...")

else:
    # Multi-threaded processing of game data
    for w_id in range(thread_num):
        t = threading.Thread(target=worker, args=(w_id, group_library, processed_game_lib, error_game_lib, group_library_num))
        t.daemon = True  # Daemonize thread
        t.start()
        workers.append(t)
        
    # Wait for all threads to finish
    for t in workers:
        t.join(timeout=30 * 60)
        if t.is_alive():
            print_log("Thread timed out. Exiting...")
            break
        else:
            print_log(f"Thread {t} finished.")

    print_log("All threads finished.")
    print_log(f"Processed {len(processed_game_lib)} games.")
    print_log(f"Error games: {len(error_game_lib)} games.")

# Process cluster scores
for game in processed_game_lib:
    append_cluster_ranking(game)



## BST Sort by score
wt_interest_ranking = tree.tree_sort(processed_game_lib)
cluster_ranking = tree.tree_sort(processed_game_lib, key='cluster_score')

# Show 10 most interesting games
print_log(f"Game Interest Ranking   : {len(wt_interest_ranking)} games")
print_log("Top 10 most interesting games:")
for idx, game in enumerate(wt_interest_ranking[-10:]):
    print(f"---------------------------------------------------------------------------------------\n")
    print(f"Game[{idx}]:{game['name']}\n    appid = {game['game']['appid']}\n    interest_score = {game['score']}\n")

print_log(f"\nCluster Interest Ranking: {len(cluster_ranking)} games")
for idx, game in enumerate(cluster_ranking[-10:]):
    print(f"---------------------------------------------------------------------------------------\n")
    print(f"Game[{idx}]:{game['name']}\n    appid = {game['game']['appid']}\n    cluster_score = {game['cluster_score']}\n")



# Dump ranking data
with open(wt_avg_game_rankings_file_path, "w") as f:
    json.dump({"wt avg rankings": wt_interest_ranking}, f, indent=2)

with open(cluster_game_rankings_file_path, "w") as f:
    json.dump({"cluster rankings": cluster_ranking}, f, indent=2)

print_log("Game rankings saved.")
### End of main