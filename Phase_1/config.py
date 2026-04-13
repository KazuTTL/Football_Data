import os
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = "sofascore.p.rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": RAPIDAPI_HOST
}

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = "us-east-1"

# MASTER DATA: 5 GIẢI VĐQG ---
TARGET_LEAGUES = {
    "Premier League": 17,
    "La Liga": 8,
    "Serie A": 23,
    "Bundesliga": 35,
    "Ligue 1": 34
}

CHAMPIONS_LEAGUE_ID = 7