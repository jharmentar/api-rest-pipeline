# config.py
import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL')

if not API_TOKEN:
    raise ValueError("API_TOKEN not configured. Create a .env file")