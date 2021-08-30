import configparser
from pathlib import Path

import os

TRIFACTA_CONFIG_PATH = str(Path.home()) + "/.trifacta.conf"
DEFAULT = "DEFAULT"


def load_config(config_path, profile):
    check_config(config_path, profile)
    config, exists = read_config(config_path, profile)
    return config


def check_config(filepath: str, profile):
    raw_config, config_exist = read_raw_config(filepath)

    if not config_exist or profile not in raw_config:
        raise (
            PermissionError(
                "Trifacta access has not been configured. Try running setup_configuration()"
            )
        )
    config, config_exist = read_config(filepath, profile)
    if config.get("type") is None:
        config["type"] = "default"

    # generate KeyError for any missing keys
    config["username"]
    config["endpoint"]
    config["token"]
    config["filesystem_root"]
    config["type"]
    return True


def setup_configuration(
    username: str, token: str, endpoint: str = "http://localhost:3005",
    filepath: str = TRIFACTA_CONFIG_PATH
):
    config = configparser.ConfigParser()

    config[DEFAULT] = {"username": username, "endpoint": endpoint, "token": token}

    with open(filepath, "w") as f:
        config.write(f)

    check_config(filepath)
    # print_raw_config(config)


def read_raw_config(filepath: str) -> (configparser.ConfigParser, bool):
    if not os.path.isfile(filepath):
        return None, False

    config = configparser.ConfigParser()
    config.read(filepath)
    return config, True


def read_config(filepath: str, profile) -> (configparser.ConfigParser, bool):
    config, config_exist = read_raw_config(filepath)
    if not config_exist:
        return None, False
    return config[profile], config_exist


# def print_raw_config(config: configparser.ConfigParser):
#     for each_section in config.sections():
#         print(each_section)
#         for (each_key, each_val) in config.items(each_section):
#             print("- ", each_key + ": ", each_val)
