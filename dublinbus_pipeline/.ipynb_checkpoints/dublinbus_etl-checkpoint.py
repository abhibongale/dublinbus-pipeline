import urllib.request
from prefect import flow, task

from config_reader import ConfigReader
from data_transformer import DataTransformer

API_SECTION="api"
API_URL_CONFIG_PARAM="url"
API_KEY_CONFIG_PARAM="key"

def get_dublin_bus_api_url():
    """
    Retrieves the Dublin Bus API URL from the configuration.
    """
    api_url = ConfigReader().read_config(API_SECTION, API_URL_CONFIG_PARAM) # fetch API call
    return api_url

def get_dublin_bus_api_headers():
    """
    Retrieves Dublin Bus API headers from the configuration.
    """
    api_key = ConfigReader().read_config(API_SECTION, API_KEY_CONFIG_PARAM) # read the API

    # Request headers
    headers = {
        'Cache-Control': 'no-cache',
        'x-api-key': api_key,
    }
    return headers

@task(log_prints=True, retries=3)
def get_dublin_bus_live_data():
    """
    Fetches live data from the Dublin Bus API.
    """
    url = get_dublin_bus_api_url()
    headers = get_dublin_bus_api_headers()
    
    try:
        req = urllib.request.Request(url, headers=headers)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        print(f"Response code {response.getcode()}")
        return response.read()
    except Exception as e:
        print(f'Extraction error: {e}')
        raise

@task(log_prints=True, retries=3)
def transform_dublin_bus_data(json_format_data):
    """
    Transforms Dublin Bus raw data (json) into tabular form.
    """
    tabular_format_data = DataTransformer().transform_data(json_format_data)
    return tabular_format_data


def load_dublin_bus_data(tabular_data):
    print(tabular_data)
    pass
    
@flow(name="dublin_bus_pipeline_flow")
def dublinbus_etl():
    
    # fetch the dublin bus live data (json format)
    dublin_bus_raw_data = get_dublin_bus_live_data()
    
    # returns data in tabular form
    dublin_bus_clean_data = transform_dublin_bus_data(dublin_bus_raw_data) 
    
    # load the clean data into database
    load_dublin_bus_data(dublin_bus_clean_data)

if __name__ == "__main__":
    dublinbus_etl.serve(name="my-first-deployment") # Run dublin bus pipeline
    #dublinbus_etl()
    