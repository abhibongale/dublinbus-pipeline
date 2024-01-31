import unittest
import configparser
from dublinbus_pipeline/config_reader import ConfigReader

class TestReadConfig(unittest.TestCase):

    def test_read_config_url(self):
        con = Config()
        URL = con.read_config(section='api',option='url',config_path="./config/config.ini")
        self.assertEqual(URL, "https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json")
    
    def test_read_config_url_isnone(self):
        con = Config()
        URL = con.read_config(section='api',option='url',config_path="./config/config.ini")
        self.assertIsNotNone(URL)    


