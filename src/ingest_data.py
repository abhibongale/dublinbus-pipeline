import urllib.request, json
import prefect as prefect
import pandas as pd

from config import read_connection

def extract_data(url, hdr):
    try:
        req = urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
    except Exception as e:
        print(f'Extraction error {e}')
        raise e
        
    return response.read()


def transform_data(raw_data):
    # Decode UTF-8 bytes to Unicode, and convert single quotes 
    # to double quotes to make it valid JSON
    my_json = raw_data.decode('utf8').replace("'", '"')
    # Load the JSON to a Python list & dump it back out as formatted JSON
    data = json.loads(my_json)
    # expand the dictionary
    df = pd.json_normalize(data['entity'])
    # remove the list from column 
    df = df.explode('trip_update.stop_time_update')
    # expand the stop_time_update
    df = pd.concat([df.drop(['trip_update.stop_time_update'], axis=1), df['trip_update.stop_time_update'].apply(pd.Series)], axis=1)
    # expand the arrival column
    df = pd.concat([df.drop(['arrival'], axis=1), df['arrival'].apply(pd.Series).rename(columns={"delay" : "arrival_delay"})['arrival_delay']], axis=1)
    # expand the departure column
    df = pd.concat([df.drop(['departure'], axis=1), df['departure'].apply(pd.Series).rename(columns={"delay" : "departure_delay"})['departure_delay']], axis=1)
    # remove nan columns
    df = df.dropna(axis=1, how="all")
    # column rename 
    df = df.rename(columns={'trip_update.trip.trip_id':'trip_id', 'trip_update.trip.start_time' : 'start_time',
       'trip_update.trip.start_date': 'start_date', 'trip_update.trip.schedule_relationship': 'trip_schedule_relationship',
       'trip_update.trip.route_id':'route_id', 'trip_update.trip.direction_id':'direction_id',
       'trip_update.vehicle.id':'vehicle_id', 'trip_update.timestamp': 'timestamp'})
    return df

# load the dataframe into table TripUpdates
def load_data(username, password, port, ipaddress, database_name, data):
    # create database
    #create_db(database_name)
    try:
        # connection engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
        engine = create_engine(f'postgresql://{username}:{password}@{ipaddress}:{port}/{database_name}')
        df.to_sql('TripUpdates', engine, if_exists='append', index=False)
        print(f' TripUpdates table in database {database_name}')
    except Exception as e:
        print(f'failed to write dataframe to {database_name}: {e}')
        raise e

def ingest_workflow(url, hdr, postgres_config):
    raw_data = extract_data(url, hdr)
    
    data = transform_data()
    load_data(postgres_config['username'], postgres_config['password'], 
    postgres_config['port'], postgres_config['ipaddress'], postgres_config['database'], data)

if __name__ == "main":
    url = "https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json"
    conn_config = read_connection(path='./config/connection.ini', section='extract')
    hdr = {
        'Cache-Control': 'no-cache',
        'x-api-key': conn_config['api']
    }
    postgres_config = config.read_connection(path="./config/connection.ini", section="postgresql")
    
    ingest_workflow(url, conn_config, hdr, postgres_config)
