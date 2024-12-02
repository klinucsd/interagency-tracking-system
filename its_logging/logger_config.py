
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    # level=logging.DEBUG,
    # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    format="%(asctime)s %(levelname)-5s [%(name)-22s]  %(message)s",
    handlers=[
        logging.StreamHandler(),  # Logs to console
        # logging.FileHandler("app.log", mode='w')  # Logs to file
    ]
)

# Optional: Retrieve the logger to ensure it's imported properly
logger = logging.getLogger(__name__)
