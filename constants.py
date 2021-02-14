import os
from dotenv import load_dotenv
load_dotenv()

API_RATE = int(os.environ.get('API_RATE'))
API_ROOT = os.environ.get('API_ROOT')
DATABASE = os.environ.get('DATABASE')
DBUSER = os.environ.get('DBUSER')
PASSWORD = os.environ.get('PASSWORD')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
