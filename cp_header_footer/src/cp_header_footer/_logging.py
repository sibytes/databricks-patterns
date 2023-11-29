import logging
import logging.config
import json
import os
from json.decoder import JSONDecodeError

def configure_logging():
    """Configures the logging based on the logging.json file in the root
    """

    log_config_file = f"{os.getcwd()}/../logging.json"


    # check that it exists
    if not os.path.exists(log_config_file):
        msg = f"Config logging file path does not exist {log_config_file}"
        raise Exception(msg)

    # load the logging configuration into the logger
    with open(log_config_file, "r") as f:
        try:
            config = json.load(f)
            logging.config.dictConfig(config)

        except JSONDecodeError as e:

            raise Exception(
                f"Invalid yaml format in {log_config_file}."
            ) from e
