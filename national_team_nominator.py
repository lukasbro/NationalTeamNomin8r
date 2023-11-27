import os
import pandas as pd
import numpy as np

def read_csv_file(folder_path, file_name):
    file_path = os.path.join(folder_path, file_name + '.csv')
    return pd.read_csv(file_path)

def filter_players(players_df):
    return players_df[(players_df['country_of_citizenship'] == 'Germany') & (players_df['last_season'] > 2022)][['player_id', 'first_name', 'last_name', 'position', 'sub_position', 'market_value_in_eur']]

def process_appearances(appearances_df):
    relevant_columns = ['player_id', 'player_current_club_id', 'yellow_cards', 'red_cards', 'goals', 'assists', 'minutes_played']
    appearances_df = appearances_df[relevant_columns]
    player_stats = appearances_df.groupby('player_id').sum().reset_index()
    return player_stats

def merge_dataframes(left_df, right_df, on_column, how_type='left'):
    return pd.merge(left_df, right_df, on=on_column, how=how_type)

def fill_nan_values(df):
    df.fillna(0, inplace=True)

def filter_by_minutes_played(df, threshold):
    return df[df['minutes_played'] > threshold]

def calculate_ratios(df):
    df = df.copy()
    df['goals_per_minute'] = df['goals'] / df['minutes_played']
    df['assists_per_minute'] = df['assists'] / df['minutes_played']
    df['cards_per_minute'] = (df['yellow_cards'] + df['red_cards']) / df['minutes_played']
    return df

def sort_dataframe(df, columns, ascending_values):
    return df.sort_values(by=columns, ascending=ascending_values)

def select_top_players(df, position_column, top_count):
    top_players = []
    for position in df[position_column].unique():
        top_players.extend(df[df[position_column] == position].head(top_count).to_dict(orient='records'))
    return pd.DataFrame(top_players)

def process_goalkeeper_data(merged_data):
    german_players_data = merged_data[(merged_data['country_of_citizenship'] == 'Germany') & (merged_data['last_season'] >= 2022)]

    grouped_data = german_players_data.groupby(['player_name', 'sub_position']).agg(
        sum_minutes_played=('minutes_played', 'sum'),
        sum_goals=('goals', 'sum'),
        sum_assists=('assists', 'sum'),
        sum_away_goals=('away_club_goals', 'sum')
    ).reset_index()

    grouped_data['lose_goals_per_minute'] = grouped_data['sum_away_goals'] / grouped_data['sum_minutes_played']
    grouped_data['lose_goals_per_minute'].replace([np.inf, -np.inf], 9999, inplace=True)

    sorted_data = grouped_data.sort_values(by='sum_goals')

    sorted_data = grouped_data.sort_values(by='lose_goals_per_minute', ascending=True)

    filtered_data = sorted_data[(sorted_data['sub_position'] == 'Goalkeeper') & (sorted_data['sum_minutes_played'] >= 1500)]

    return filtered_data.head(4)

def main():
    script_directory = os.path.dirname(__file__)
    data_folder_path = os.path.join(script_directory, '../Data/Data_national_team_nominator')

    players_df = read_csv_file(data_folder_path, 'players')
    german_players_df = filter_players(players_df)

    appearances_df = read_csv_file(data_folder_path, 'appearances')
    player_stats_df = process_appearances(appearances_df)

    german_players_df = merge_dataframes(german_players_df, player_stats_df, 'player_id')

    fill_nan_values(german_players_df)

    german_players_df_filtered = filter_by_minutes_played(german_players_df, 1500)

    german_players_df_filtered = calculate_ratios(german_players_df_filtered)

    german_players_df_sorted = sort_dataframe(german_players_df_filtered, ['position', 'goals_per_minute'], [True, False])

    top_players_df = select_top_players(german_players_df_sorted, 'sub_position', 4)

    print(top_players_df)

    games_df = read_csv_file(data_folder_path, 'games')
    merged_data = pd.merge(players_df, appearances_df, on='player_id')
    merged_data = pd.merge(merged_data, games_df, on='game_id')

    top_goalkeepers = process_goalkeeper_data(merged_data)
    print(top_goalkeepers)

if __name__ == "__main__":
    main()