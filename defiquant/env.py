import os
from dotenv import load_dotenv

load_dotenv()  # loads .env in root dir by default

# Explicit variables
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
VAULTSFYI_KEY = os.getenv('VAULTSFYI')
FLIPSIDE_API_KEY = os.getenv("FLIPSIDE_API_KEY")
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
FRED_API_KEY = os.getenv("FRED_API_KEY")

def load_environment_variables():
    return {
        "COINGECKO_API_KEY": COINGECKO_API_KEY,
        "VAULTSFYI_KEY": VAULTSFYI_KEY,
        "FLIPSIDE_API_KEY": FLIPSIDE_API_KEY,
        "DUNE_API_KEY": DUNE_API_KEY,
        "FRED_API_KEY": FRED_API_KEY,
    }
