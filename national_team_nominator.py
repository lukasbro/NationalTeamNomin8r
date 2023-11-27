import pandas as pd
import numpy as np
import os

script_directory = os.path.dirname(__file__)
data_folder_path = os.path.join(script_directory, '../Data/Data_national_team_nominator')

# Liste aller CSV-Dateien im Ordner
csv_files = [
    'game_lineups.csv',
    'competitions.csv',
    'appearances.csv',
    'player_valuations.csv',
    'game_events.csv',
    'players.csv',
    'games.csv',
    'club_games.csv',
    'clubs.csv'
]

"""
# Iteriere über jede CSV-Datei und drucke die Spaltennamen
for csv_file in csv_files:
    csv_file_path = os.path.join(data_folder_path, csv_file)
    df = pd.read_csv(csv_file_path)
    
    print(f"\nSpaltennamen für {csv_file}:\n")
    print(df.columns)
"""

players_file_path = os.path.join(data_folder_path, 'players.csv')
players_df = pd.read_csv(players_file_path)
# print(players_df.columns)

# Filtere Spieler mit Staatsbürgerschaft "Germany" und last_season > 2022
german_players_df = players_df[(players_df['country_of_citizenship'] == 'Germany') & (players_df['last_season'] > 2022)][['player_id', 'first_name', 'last_name', 'position', 'sub_position', 'market_value_in_eur']]
# print(german_players_df)

# Lese die appearances.csv ein
appearances_file_path = os.path.join(data_folder_path, 'appearances.csv')
appearances_df = pd.read_csv(appearances_file_path)

# Filtere nur die relevanten Spalten
appearances_df = appearances_df[['player_id', 'player_current_club_id', 'yellow_cards', 'red_cards', 'goals', 'assists', 'minutes_played']]

# Gruppiere nach 'player_id' und summiere die Werte
player_stats = appearances_df.groupby('player_id').sum().reset_index()

# Füge die kumulierten Werte zu german_players_df hinzu
german_players_df = pd.merge(german_players_df, player_stats, on='player_id', how='left')

# Fülle NaN-Werte mit 0 auf (wenn ein Spieler in 'german_players_df' existiert, aber keine Auftritte hatte)
german_players_df.fillna(0, inplace=True)

# Filtere Spieler mit weniger als 1500 minutes_played
german_players_df_filtered = german_players_df[german_players_df['minutes_played'] > 1500]

# Berechne goals_per_minute, assists_per_minute und cards_per_minute
german_players_df_filtered = german_players_df_filtered.copy()
german_players_df_filtered['goals_per_minute'] = german_players_df_filtered['goals'] / german_players_df_filtered['minutes_played']
german_players_df_filtered['assists_per_minute'] = german_players_df_filtered['assists'] / german_players_df_filtered['minutes_played']
german_players_df_filtered['cards_per_minute'] = (german_players_df_filtered['yellow_cards'] + german_players_df_filtered['red_cards']) / german_players_df_filtered['minutes_played']

# Sortiere den DataFrame nach Position und goals_per_minute absteigend
german_players_df_sorted = german_players_df_filtered.sort_values(by=['position', 'goals_per_minute'], ascending=[True, False])

# Erstelle eine leere Liste, um die besten vier Spieler für jede Position zu speichern
top_players_by_position = []

# Iteriere über jede Position und wähle die besten vier Spieler aus
for position in german_players_df_sorted['sub_position'].unique():
    top_players_by_position.extend(german_players_df_sorted[german_players_df_sorted['sub_position'] == position].head(4).to_dict(orient='records'))

# Erstelle einen DataFrame aus der Liste der besten Spieler nach Position
top_players_df = pd.DataFrame(top_players_by_position)

# Drucke den DataFrame mit den besten vier Spielern für jede Position
print(top_players_df)



# Lese die CSV-Dateien ein
players = pd.read_csv(players_file_path)
appearances = pd.read_csv(appearances_file_path)
games_file_path = os.path.join(data_folder_path, 'games.csv')
games = pd.read_csv(games_file_path)

# Führe einen INNER JOIN zwischen players, appearances und games durch
merged_data = pd.merge(players, appearances, on='player_id')
merged_data = pd.merge(merged_data, games, on='game_id')  # Stelle sicher, dass 'game_id' in 'appearances' vorhanden ist

# Filtere nach deutschen Spielern und der Saison 2021
german_players_data = merged_data[(merged_data['country_of_citizenship'] == 'Germany') & (merged_data['last_season'] >= 2022)]

# Gruppiere nach 'player_name' und 'sub_position' und berechne die Summen
grouped_data = german_players_data.groupby(['player_name', 'sub_position']).agg(
    sum_minutes_played=('minutes_played', 'sum'),
    sum_goals=('goals', 'sum'),
    sum_assists=('assists', 'sum'),
    sum_away_goals=('away_club_goals', 'sum')
).reset_index()

# Füge die LoseGoalsPerMinute-Spalte hinzu
grouped_data['lose_goals_per_minute'] = grouped_data['sum_away_goals'] / grouped_data['sum_minutes_played'] 
grouped_data['lose_goals_per_minute'].replace([np.inf, -np.inf], 9999, inplace=True)

# print(grouped_data)

# Sortiere nach LoseGoalsPerMinute
sorted_data = grouped_data.sort_values(by='sum_goals')

# Drucke das Ergebnis
# print(sorted_data[['player_name', 'sub_position', 'sum_minutes_played', 'sum_goals', 'sum_assists', 'lose_goals_per_minute']])

# Sortiere nach lose_goals_per_minute, mit dem Spieler mit dem höchsten Wert zuerst
sorted_data = grouped_data.sort_values(by='lose_goals_per_minute', ascending=True)

# Filtere nach sub_position "Goalkeeper" und mindestens 1500 Minuten gespielt
filtered_data = sorted_data[(sorted_data['sub_position'] == 'Goalkeeper') & (sorted_data['sum_minutes_played'] >= 1500)]

# Drucke das Ergebnis
# print(filtered_data[['player_name', 'sub_position', 'sum_minutes_played', 'sum_goals', 'sum_assists', 'lose_goals_per_minute']])

# Wähle die besten vier Torhüter aus
top_goalkeepers = filtered_data.head(4)

# Drucke das Ergebnis
# print(top_goalkeepers[['player_name', 'sub_position', 'sum_minutes_played', 'sum_goals', 'sum_assists', 'lose_goals_per_minute']])
print(top_goalkeepers)
