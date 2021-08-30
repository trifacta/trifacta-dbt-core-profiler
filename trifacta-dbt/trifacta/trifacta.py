import os
import yaml
import requests
from openapi_client import ApiClient, Configuration


DEFAULT_TRIFACTA_CONFIG_DIR = os.path.join(os.path.expanduser('~'), '.trifacta')
TRIFACTA_CONFIG_DIR = os.path.expanduser(DEFAULT_TRIFACTA_CONFIG_DIR)


class TrifactaConfig:
    def __init__(self, config_dir, profile):
        config_path = os.path.abspath(config_dir + '/config.yml')
        with open(config_path, "r") as stream:
            self.config = yaml.safe_load(stream)
        self.active_profile = self.config['profiles'][profile]


class Trifacta:
    def __init__(self, config):
        self.config = config
        self.api_config = Configuration(
            host=self.config.active_profile['url'],
            access_token=self.config.active_profile['api_key']
        )
        self.api_config.verify_ssl = False
        self.api_config.debug = True
        self.api_client = ApiClient(configuration=self.api_config)
