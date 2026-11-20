import logging
import snowflake.connector

from config.config import SNOWFLAKE_CONFIG

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Snowflake connection
# -------------------------------------------------------------------

def create_snowflake_connection():
    """
    Creates a Snowflake connection using key-pair authentication

    The private key is read from the mounted secrets directory
    It is never exposed in the logs
    """

    logger.info("Connecting to Snowflake...")

    connection = snowflake.connector.connect(
        **SNOWFLAKE_CONFIG,
    )

    logger.info("Successfully connected to Snowflake")

    return connection