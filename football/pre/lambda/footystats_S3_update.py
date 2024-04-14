import requests
import pandas as pd
from io import StringIO
import boto3

class APIClient:
    """Football data API client"""

    BASE_URL = 'https://api.football-data-api.com'

    def __init__(self, key):
        self.key = key

    def _make_request(self, endpoint, params=None):
        """General method for making requests"""
        params = params or {}
        params = {'key': self.key, **params} 
        response = requests.get(f'{self.BASE_URL}/{endpoint}', params=params)


        response.raise_for_status()

        return response.json()['data']

class LeagueDataClient(APIClient):
    """API client for getting League data"""

    def get_league_list(self, **kwargs):
        return self._make_request('league-list', params=kwargs)

    def get_country_list(self):
        return self._make_request('country-list')

    def get_todays_matches(self, **kwargs):
        return self._make_request('todays-matches', params=kwargs)

    def get_league_matches(self, season_id, **kwargs):
        kwargs.update({'season_id': season_id})
        return self._make_request('league-matches', params=kwargs)

    def get_league_season(self, season_id, **kwargs):
        kwargs.update({'season_id': season_id})
        return self._make_request('league-season', params=kwargs)

    def get_league_teams(self, season_id, **kwargs):
        kwargs.update({'season_id': season_id})
        return self._make_request('league-teams', params=kwargs)

    def get_league_players(self, season_id, **kwargs):
        kwargs.update({'season_id': season_id})
        return self._make_request('league-players', params=kwargs)

    def get_league_referees(self, season_id, **kwargs):
        kwargs.update({'season_id': season_id})
        return self._make_request('league-referees', params=kwargs)

    def get_team(self, team_id):
        return self._make_request('team', {'team_id': team_id})
    
    def get_lastx(self, team_id):
        return self._make_request('lastx', {'team_id': team_id})

    def get_match_stats(self, match_id):
        return self._make_request('match', {'match_id': match_id})
    

class GenDataFrame():
    """General class for creating dataframes from API data"""
    
    def __init__(self, client, league_name, country, years):
        self.client = client
        self.league_name = league_name
        self.country = country
        self.years = years

    def get_league_list_df(self):
        response = self.client.get_league_list()
        leagues_df = pd.DataFrame(response)
        return leagues_df

    def get_matches_by_league_df(self, season_id, max_per_page=None, page=None, max_time=None):
        params = {
            'max_per_page': max_per_page,
            'page': page,
            'max_time':max_time
        }
        response = self.client.get_league_matches(season_id, **params)
        matches_by_league_df = pd.DataFrame(response)
        return matches_by_league_df

    def get_filtered_leagues(self, league_name, country, years):
        leagues = self.get_league_list_df()
        filtered_leagues = leagues[(leagues['country'] == country) & (leagues['league_name'] == league_name)]
        
        all_leagues_data = []

        for _, row in filtered_leagues.iterrows():
            for season in row['season']:
                match_data = {
                    'id': season['id'],
                    'year': str(season['year'])[:4],
                    'league_name': row['league_name'],
                    'country': row['country']
                }
                all_leagues_data.append(match_data)
        
        all_leagues_data = pd.DataFrame(all_leagues_data)
        all_leagues_data = all_leagues_data[all_leagues_data['year'].isin(years)]

        return all_leagues_data

    def get_footystats_matches(self, league_name, country, years):
        leagues = self.get_filtered_leagues(league_name, country, years)
        all_matches = pd.DataFrame()

        for _, row in leagues.iterrows():
            matches = self.get_matches_by_league_df(row['id'])
            matches['season_id'] = row['id']
            matches['league_name'] = row['league_name']
            matches['country'] = row['country']
            matches['year'] = row['year']
            all_matches = pd.concat([all_matches, matches], ignore_index=True)

        return all_matches

    def get_referee_map_for_seasons(self, season_ids):
        referee_map = {}
        for season_id in season_ids:
            referees = self.client.get_league_referees(season_id)

            for ref in referees:
                referee_map[ref['id']] = ref['full_name']

        return referee_map

    def get_team_images_for_ids(self, team_ids):
        team_image_map = {}
        
        for team_id in team_ids:
            team_data = self.client.get_team(team_id)
            if team_data and 'image' in team_data[0]:
                team_image_map[team_id] = team_data[0]['image']
        
        return team_image_map

    def main(self):
        all_match_data = self.get_footystats_matches(self.league_name, self.country, self.years)
        unique_season_ids = all_match_data['season_id'].unique()
        referee_map = self.get_referee_map_for_seasons(unique_season_ids)
        all_match_data['referee_name'] = all_match_data['refereeID'].map(referee_map)
        unique_team_ids = all_match_data['homeID'].unique()
        team_image_map = self.get_team_images_for_ids(unique_team_ids)
        all_match_data['home_team_image'] = all_match_data['homeID'].map(team_image_map)
        all_match_data['away_team_image'] = all_match_data['awayID'].map(team_image_map)
        return all_match_data

def update_csv_in_s3(df, bucket_name, file_key, aws_access_key_id=None, aws_secret_access_key=None):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)  
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

def run_footystats_data_process(api_key, league_name, country, years, bucket_name, file_key, aws_key, aws_secret):
    
    client = LeagueDataClient(api_key)
    new_df = GenDataFrame(client, league_name, country, years).main()

    new_df.to_csv('newfootystats.csv', index=False)

    update_csv_in_s3(new_df, bucket_name, file_key, aws_key, aws_secret)
    print("Updated CSV in S3")

def main():
    # Parameters
    api_key = "your_api_key"
    league_name = "Premier League"
    country = "England"
    years =  ['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023']

    run_footystats_data_process(api_key, league_name, country, years, bucket_name, file_key, aws_key, aws_secret)

if __name__ == "__main__":
    main()