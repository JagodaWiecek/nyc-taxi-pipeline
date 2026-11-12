from src.config import Config
from src.clients import TlcClient
import logging

if __name__ == "__main__":
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
    )

    config = Config()
    client = TlcClient(config)
    print("Client created:", client.session)
