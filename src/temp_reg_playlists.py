from mongo_adapter import save_data_mongo_by_id

data = [
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Melodic", "sp_id": "7DsEkY12jYUOx8ybyVpxNf"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Party", "sp_id": "0TU0qwOLWGRqMl3iT4EIH2"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Hard", "sp_id": "53tGlt3aqYPCwHHiuR5Ir0"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Shadowy", "sp_id": "3TBmjMrPebbDSwkj0e3qvy"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "ReDrum", "sp_id": "5jpb0hGPlxsp0We6ywbX5k"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Eastern", "sp_id": "2CGh4YeNYVM2ZAQ7d8tMie"},
    {"clouder_style": "DNB", "clouder_type": "heap", "clouder_playlist": "Alt", "sp_id": "1PJ4RCEsKieSnbVEew2G53"},
]

for pl in data:
    pl["id"] = f"{pl['clouder_style']}_{pl['clouder_playlist']}_{pl['clouder_type']}"

save_data_mongo_by_id(data, "clouder_playlists")