import logging
import sys
from clickhouse_driver import Client, connect
from config import connection_str


def _configure_logger() -> logging.Logger:
    """
    This function handles the logging setup which will replace print.
    Logging can be used for debugging purposes or just info printing
    P > S - Set to logging.DEBUG mode to see how the script runs in some cases.
    :return: An instance of logging as logger
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - Data Rebalance - %(message)s "
    )
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def ch_connection():
    connection = connect(connection_str)
    cursor = connection.cursor()
    return connection, cursor


def _close_con(con_cursor, con_connection) -> None:
    con_cursor.close()
    con_connection.close()
    _log_set.info("DB connection is closed")


def generator_to_str(gen):
    output = ""
    for line in gen:
        output += line + "\n"
    return output


########################
# _logger Global scope #
########################
_log_set = _configure_logger()
