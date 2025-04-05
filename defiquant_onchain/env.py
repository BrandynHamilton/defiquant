import os
from dotenv import load_dotenv

load_dotenv()  # loads .env in root dir by default

# Account ; ideally we can add more accounts to our web3 object
ACCOUNT_ADDRESS = os.getenv('ACCOUNT_ADDRESS')
ACCOUNT_KEY = os.getenv('ACCOUNT_KEY')

#Main gateways
AVALANCHE_GATEWAY = os.getenv('AVALANCHE_GATEWAY')
GNOSIS_GATEWAY = os.getenv('GNOSIS_GATEWAY')
ARBITRUM_GATEWAY = os.getenv('ARBITRUM_GATEWAY')
BASE_GATEWAY = os.getenv('BASE_GATEWAY')
OPTIMISM_GATEWAY = os.getenv('OPTIMISM_GATEWAY')
ETHEREUM_GATEWAY = os.getenv('ETHEREUM_GATEWAY')
POLYGON_GATEWAY = os.getenv('POLYGON_GATEWAY')

#Backup gateways
AVALANCHE_BACKUP = os.getenv('AVALANCHE_BACKUP')
GNOSIS_BACKUP = os.getenv('GNOSIS_BACKUP')
ARBITRUM_BACKUP = os.getenv('ARBITRUM_BACKUP')
BASE_BACKUP = os.getenv('BASE_BACKUP')
OPTIMISM_BACKUP = os.getenv('OPTIMISM_BACKUP')
ETHEREUM_BACKUP = os.getenv('ETHEREUM_BACKUP')
POLYGON_BACKUP = os.getenv('POLYGON_BACKUP')

def load_environment_variables():
    return {
        # Account
        "ACCOUNT_ADDRESS": ACCOUNT_ADDRESS,
        "ACCOUNT_KEY": ACCOUNT_KEY,

        # Main gateways
        "AVALANCHE_GATEWAY": AVALANCHE_GATEWAY,
        "ARBITRUM_GATEWAY": ARBITRUM_GATEWAY,
        "BASE_GATEWAY": BASE_GATEWAY,
        "OPTIMISM_GATEWAY": OPTIMISM_GATEWAY,
        "ETHEREUM_GATEWAY": ETHEREUM_GATEWAY,
        "POLYGON_GATEWAY": POLYGON_GATEWAY,
        "GNOSIS_GATEWAY":GNOSIS_GATEWAY,

        # Backup gateways
        "AVALANCHE_BACKUP": AVALANCHE_BACKUP,
        "ARBITRUM_BACKUP": ARBITRUM_BACKUP,
        "BASE_BACKUP": BASE_BACKUP,
        "OPTIMISM_BACKUP": OPTIMISM_BACKUP,
        "ETHEREUM_BACKUP": ETHEREUM_BACKUP,
        "POLYGON_BACKUP": POLYGON_BACKUP,
        "GNOSIS_BACKUP": GNOSIS_BACKUP
    }
