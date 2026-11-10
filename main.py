from src.config import Config
from src.clients import TlcClient

if __name__ == "__main__":
    config = Config()
    client = TlcClient(config)
    print("Client created:", client.session)