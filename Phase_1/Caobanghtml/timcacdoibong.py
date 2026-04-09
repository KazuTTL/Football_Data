import json

# 1. Đọc file JSON
with open(r"C:\Users\Tien Loc\Downloads\sofascore_big5_clean_20260406_1033.json", "r", encoding="utf-8") as f:
    sofa_data = json.load(f)

unique_teams = set()

for player in sofa_data.get('data', []):
    detailed_stats = player.get('detailed_statistics', {})
    domestic_league = detailed_stats.get('domestic_league', {})
    team_info = domestic_league.get('team', {})
    team_name = team_info.get('name')
    
    if team_name:
        unique_teams.add(team_name)

print("\nDANH SÁCH ĐỘI BÓNG ĐÃ LẤY THÀNH CÔNG:")
list(unique_teams)
print(unique_teams)
print(f'Số lượng các đội: {len(unique_teams)}')