import configparser
import os.path

class ConfigReader:
    """
    Reads configuration from a file.
    """

    def __init__(self):
        self.config = configparser.ConfigParser()

    def read_config(self, section, option, config_path="./config/config.ini"):
        """
        Reads a configuration parameter from the specified section and option.

        Args:
        - section (str): The section in the configuration file.
        - option (str): The parameter name.
        - config_path (str): The path to the configuration file. Default is "./config/config.ini".

        Returns:
        - The value of the specified key.

        Raises:
        - FileNotFoundError: If the specified configuration file does not exist.
        - KeyError: If the specified option is not found in the configuration file.
        """
        self._validate_config_file(config_path)
        return self._get_config_value(section, option, config_path)

    def _validate_config_file(self, config_path):
        """
        Validates the existence of the specified configuration file.

        Args:
        - config_path (str): The path to the configuration file.

        Raises:
        - FileNotFoundError: If the specified configuration file does not exist.
        """
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Configuration file '{config_path}' does not exist.")

    def _get_config_value(self, section, option, config_path):
        """
        Retrieves the value of the specified configuration option.

        Args:
        - section (str): The section in the configuration file.
        - option (str): The parameter name.

        Returns:
        - The value of the specified key.

        Raises:
        - KeyError: If the specified option is not found in the configuration file.
        """
        try:
            self.config.read(config_path)
            return self.config.get(section, option)
        except KeyError as e:
            raise KeyError(f"Option '{option}' not found in section '{section}' of the configuration file.")

# Example Usage:
# config_reader = ConfigReader()
# value = config_reader.read_config("section_name", "option_name")
