import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time

# ScraperAPI configuration
api_key = 'a5213defa72fc6234f9df1cc769270b3'
base_url = "https://fbref.com"
main_url = "https://fbref.com/en/comps/Big5/Big-5-European-Leagues-Stats"
scraperapi_url = "http://api.scraperapi.com"

def fetch_page(url, retries=3):
    """Fetches a page using ScraperAPI with retries."""
    params = {
        'api_key': api_key,
        'url': url
    }
    for attempt in range(retries):
        try:
            response = requests.get(scraperapi_url, params=params, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2)  # Add a delay before retrying
            continue
    return None  # If all retries fail, return None

try:
    # Fetch main page content
    response = fetch_page(main_url)
    if response is None:
        print("Failed to fetch the main page after multiple attempts.")
        exit()
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the container div and table
    container_div = soup.find('div', {'id': 'div_big5_table'})
    if container_div:
        teams_table = container_div.find('table', {'id': 'big5_table'})
    else:
        print("Container div with id 'div_big5_table' not found.")
        exit()

    if teams_table is None:
        print("Table with id 'big5_table' not found.")
        exit()

    # Extract team links and names
    team_links = [(a['href'].replace('/2023-2024/', '/2023-2024/all_comps/'), a.getText(strip=True)) 
                  for a in teams_table.find_all('a', href=True) if '/squads/' in a['href']]

    # Initialize empty lists to store data
    all_players_data = []

    # Iterate over each team's link to fetch player stats
    for team_link, club_name in team_links:
        team_url = base_url + team_link
        team_response = fetch_page(team_url)
        if team_response is None:
            print(f"Failed to fetch {team_url} after multiple attempts.")
            continue

        team_soup = BeautifulSoup(team_response.content, 'html.parser')

        # Print the URL being processed for debugging
        print(f"Processing {team_url} for club {club_name}")

        # Find the table containers for basic and advanced keeper stats
        adv_keeper_stats_table = team_soup.find('div', {'id': 'div_stats_keeper_adv_combined'})

        for table_container in [adv_keeper_stats_table]:
            if table_container:
                player_stats_table = table_container.find('table')
                if player_stats_table:
                    # Extract column headers
                    headers = [th.getText() for th in player_stats_table.find_all('th')[1:]]  # Skipping first empty header
                    headers.insert(0, 'Player Name')
                    headers.insert(1, 'Club Name')

                    # Extract player stats rows
                    rows = player_stats_table.find_all('tr')
                    for row in rows:
                        player_name_tag = row.find('th', {'data-stat': 'player'})
                        player_name = player_name_tag.getText() if player_name_tag else 'N/A'

                        player_data = [td.getText() for td in row.find_all('td')]
                        if player_data:  # Skip empty rows
                            player_data.insert(0, player_name)
                            player_data.insert(1, club_name)
                            all_players_data.append(dict(zip(headers, player_data)))

    # Convert the list of dictionaries to DataFrame
    df = pd.DataFrame(all_players_data)

    # Save DataFrame to CSV
    df.to_csv('all_teams_adv_keeper_stats_all_comps.csv', index=False)

    # Save DataFrame to JSON
    df.to_json('all_teams_adv_keeper_stats_all_comps.json', orient='records', lines=True)

    print("Data has been successfully saved to 'all_teams_adv_keeper_stats_all_comps.csv' and 'all_teams_adv_keeper_stats_all_comps.json'.")

except requests.RequestException as e:
    print(f"Failed to fetch the main page: {e}")
