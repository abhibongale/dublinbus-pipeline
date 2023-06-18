from configparser import ConfigParser
"""
read the connection ini
@param path: ini file path
@param section: section which contains the regardingp information
return: connection secret parameters e.g. api key.
"""
def read_connection(path, section):
    parser = ConfigParser() # create a parser
    parser.read(path) # read the configuration ini
    
    api_conn = {} # connection dictonary

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            api_conn[param[0]] = param[1]
    else:
        raise Exception(f'section {section} not found in {path}')
    
    return api_conn


if __name__ == 'main':
    conn_path = "./config/connection.ini"
    section = "extract"
    read_connection(conn_path, section)