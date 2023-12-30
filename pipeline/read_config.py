import configparser # read config files
import os.path

class Config:
    """
    Read Configuration
    """
    def __init__(self):
        self.config = configparser.ConfigParser()
        
    def read_config(self, section, option, config_path="./config/config.ini"):
        """
        read_config returns a string, the configurable parameter used to configure the pipeline
        Args:
        - config_path: predefined, contains the string defining the configuration file path
        - section: configuration parameter belongs to the section
        - option: parameter name
        Returns:
        - value of the existing key
        """
        try:
            if os.path.isfile(config_path):
                self.config.read(config_path)
            else:
                raise Exception(f"{config_path} file does not exist")
        except Exception as e:
            raise Exception (f"{config_path} file does not exist")
        
        try:
            return self.config.get(section, option)
        except Exception as e:
            raise Exception (f"{option} not found in {config_path}")

