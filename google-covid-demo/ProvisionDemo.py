###########################################################
# This script downloads Google Covid open dataset and load
# the data into Couchbase bucket.
# Author: Liang Chen
# email: liang.chen@couchbase.com
###########################################################

import os
import sys
import glob
import json
import requests

#########################################################################
# Initializing script execution context
#########################################################################
debug_mode = False
force_load_subregions = False
download_dataset = True
load_couchbase = True
configure_couchbase = False
if len(sys.argv) > 0:
    if sys.argv.__contains__("debug"): 
        debug_mode = True
        print("Initializing script execution contenxt...")
    if sys.argv.__contains__("force_load_subregions"): 
        force_load_subregions = True
        if debug_mode: print("All subregion data will be loaded.")
    if sys.argv.__contains__("skip_download"): 
        download_dataset = False
        if debug_mode: print("Skip downloading data from Google.")
    if sys.argv.__contains__("skip_load_couchbase"): 
        load_couchbase = False
        if debug_mode: print("Skip loading data into Couchbase.")
    if sys.argv.__contains__("configure_couchbase"): 
        configure_couchbase = True
        if debug_mode: print("Create Couchbase bucket, GSI indexes and Analytics Service datasets.")
    if sys.argv.__contains__("help"): 
        print("python " + __file__ + " [debug] [force_load_subregions] [skip_download] [skip_load_couchbase] [configure_couchbase] [help]")
        print("Options:")
        print("   debug                  By default debug is off. Include\"debug\" in the argument to enable\r\n" \
            + "                          debug level output.")
        print("   force_load_subregions  By default config.json file controls whether subregion level data \r\n" \
            + "                          will be loaded. Include \"load_all\" to overwrite the settings in \r\n" \
            + "                          config.json file and force loading subregion level data from all \r\n"  \
            + "                          levels.")
        print("   skip_download          By default the script will always download the latest data from \r\n" \
            + "                          Google. Include \"skip_download\" to skip downloading step.")
        print("   skip_load_couchbase    By default the script will always flush and reload the downloaded \r\n" \
            + "                          data file into the Couchbase bucket. Include \"skip_load_couchbase\"\r\n" \
            + "                          to skip loading Couchbase step.")
        print("   configure_couchbase    By default the script assumes the bucket and analytics service\r\n" \
            + "                          dataset are pre-configured. Include \"configure_couchbase\" to\r\n" \
            + "                          create bucket, gsi indexes and analytics service datasets.")
        quit()

#########################################################################
# Shared functions
#########################################################################
def print_message(message, is_debug_message = False):
    if debug_mode and is_debug_message: 
        print("(Debug)" + message)
    else:
        print(message)

def prepare_directory(path):
    if not os.path.exists(path):
        print_message("Create directory \"" + path + "\" because it is not exist.", True)
        os.makedirs(path)

    _files = glob.glob(path + '*')
    for _file in _files:
        try:
            print_message("Deleting file \"" + _file + "\".", True)
            os.remove(_file)
        except OSError as e:
            print_message("Error: %s : %s" % (_file, e.strerror))

def process_file(doc_type, file_url, temp_path, output_path, load_subregion):
    _file_path_temp = temp_path + doc_type + ".json"
    _file_path_output = output_path + doc_type + ".json"
    if debug_mode:
        print_message("Raw data:    " + _file_path_temp, True)
        print_message("Output file: " + _file_path_output, True)
        print_message("Downloading file from Google...", True)

    _request = requests.get(file_url, allow_redirects=True)
    open(_file_path_temp, 'wb').write(_request.content)
    
    print_message("Reading file \"" + _file_path_temp + "\".", True)
    with open(_file_path_temp) as _file_raw: 
        _data_input = json.load(_file_raw)
    
    _attribute_names = _data_input['columns']
    print_message("Found following keys from the file:\r\n" + str(_attribute_names), True)
    
    _row_number = 0
    with open(_file_path_output, 'w+') as _file_output:
        for _row in _data_input['data']:
            _row_number += 1
            _col = 0
            _row_new = {}
            while _col < len(_attribute_names):
                _attribute_name = _attribute_names[_col]
                _attribute_value = _row[_col]
                if _attribute_value is None: _attribute_value = 0
                _row_new[_attribute_name] = _attribute_value
                _col += 1
            _row_new["type"] = doc_type
            if _row_new["key"].find("_") == -1: 
                _file_output.write('%s\n' % json.dumps(_row_new))
            else:
                if load_subregion: _file_output.write('%s\n' % json.dumps(_row_new))
            if _row_number % 1000000 == 0:
                print_message("{row_number:,} records were processed.".format(row_number = _row_number), True)
        print_message("{row_number:,} records were processed.".format(row_number = _row_number), True)

def check_bucket(host_address, user_name, password, bucket_name):
    _api_endpoint = "http://" + host_address + ":8091/pools/default/buckets/" + bucket_name
    print_message("Api endpoint to check bucket is:\r\n" + _api_endpoint, True)

    _response = requests.get(_api_endpoint, auth = (user_name, password))
    if _response.status_code == 200:
        print_message("Bucket \"" + bucket_name + "\" is found.", True)
        return True
    else:
        print_message("Bucket \"" + bucket_name + "\" does not exists.")
        print_message(str(_response.status_code))
        print_message(_response.text)
        return False

def read_config_file(file_path_config):
    _config_def = {}
    try:
        with open(file_path_config) as _file_config:
            _config_def = json.load(_file_config)
    except IOError:
        print_message("File \"" + file_path_config + "\" is not accessible.")
        quit()
    
    if len(_config_def) == 0:
        print_message("Configuration is empty. Please check file \"" + file_path_config + "\".")
        quit()
    else:
        return _config_def

def create_bucket(host_address, user_name, password, bucket_definition_file):
    print_message("Bucket definition file is \"" + bucket_definition_file + "\".", True)
    _bucket_def = read_config_file(bucket_definition_file)

    _bucket_name = _bucket_def["bucketName"]
    _bucket_type = _bucket_def["bucketType"]
    _bucket_size_mb = _bucket_def["ramQuotaMB"]
    _bucket_replica_number = _bucket_def["replicaNumber"]
    _bucket_flush_enabled = _bucket_def["flushEnabled"]

    print_message("Bucket name:       " + _bucket_name, True)
    print_message("Bucket type:       " + _bucket_type, True)
    print_message("Bucket size:       " + str(_bucket_size_mb), True)
    print_message("Number of replica: " + str(_bucket_replica_number), True)
    print_message("Enable flush:      " + str(_bucket_flush_enabled), True)

    _api_endpoint = "http://" + host_address + ":8091/pools/default/buckets/"
    print_message("Api endpoint to create bucket is:\r\n" + _api_endpoint, True)

    _params = {}
    _params["name"] = _bucket_name
    _params["bucketType"] = _bucket_type
    _params["ramQuotaMB"] = _bucket_size_mb
    _params["replicaNumber"] = _bucket_replica_number
    _params["flushEnabled"] = _bucket_flush_enabled

    _response = requests.post(_api_endpoint, data = _params, auth = (user_name, password))
    if _response.status_code == 202:
        print_message("Bucket \"" + _bucket_name + "\" was created.", True)
    else:
        print_message("Failed to create bucket \"" + _bucket_name + "\".")
        print_message(str(_response.status_code), True)
        print_message(_response.text, True)
        return False

def create_gsi_indexes(host_address, user_name, password, bucket_definition_file):
    print_message("Bucket definition file is \"" + bucket_definition_file + "\".", True)
    _bucket_def = read_config_file(bucket_definition_file)    
    _gsi_def = _bucket_def["gsiDefinitions"]

    _api_endpoint = "http://" + host_address + ":8093/query/service"
    print_message("Api endpoint to create index is:\r\n" + _api_endpoint, True)

    for _gsi_definition in _gsi_def:
        _gsi_name = _gsi_definition["name"]
        _gsi_statement = _gsi_definition["definition"]
        print_message("Creating index \"" + _gsi_name + "\"")
        print_message("Statement:\r\n" + _gsi_statement, True)
        _params = {}
        _params["statement"] = _gsi_statement
        _response = requests.post(_api_endpoint, data = _params, auth = (user_name, password))
        if _response.status_code == 200:
            print_message("GSI \"" + _gsi_name + "\" was created successfully.", True)
        else:
            print_message("Failed to create GSI \"" + _gsi_name + "\".")
            print_message(str(_response.status_code), True)
            print_message(_response.text, True)


def create_as_datasets(host_address, user_name, password, cbas_datasets_definition_file_path):
    print_message("Analytics dataset definition file is \"" + cbas_datasets_definition_file_path + "\".", True)
    _datasets_def = read_config_file(cbas_datasets_definition_file_path)
        
    _api_endpoint = "http://" + host_address + ":8095/analytics/service"
    print_message("Api endpoint to create anaytics service dataset is:\r\n" + _api_endpoint, True)

    for _statement in _datasets_def["prepareStatements"]:
        _params = {}
        _params["statement"] = _statement    
        _response = requests.post(_api_endpoint, data = _params, auth = (user_name, password))
        if _response.status_code == 200:
            print_message("Preparation statement \"" + _statement + "\" was completed successfully.", True)
        else:
            print_message("Failed to execute preparation statement \"" + _statement + "\".")
            print_message(str(_response.status_code), True)
            print_message(_response.text, True)
    
    for _dataset in _datasets_def["datasets"]:
        _dataset_name = (_dataset["name"])
        _dataset_statement = _dataset["definition"]
        _params = {}
        _params["statement"] = _dataset_statement    
        _response = requests.post(_api_endpoint, data = _params, auth = (user_name, password))
        if _response.status_code == 200:
            print_message("Dataset \"" + _dataset_name + "\" was created successfully.", True)
        else:
            print_message("Failed to create dataset \"" + _dataset_name + "\".")
            print_message(str(_response.status_code), True)
            print_message(_response.text, True)

    for _statement in _datasets_def["completeStatements"]:
        _params = {}
        _params["statement"] = _statement    
        _response = requests.post(_api_endpoint, data = _params, auth = (user_name, password))
        if _response.status_code == 200:
            print_message("Completion statement \"" + _statement + "\" was completed successfully.", True)
        else:
            print_message("Failed to execute completion statement \"" + _statement + "\".")
            print_message(str(_response.status_code), True)
            print_message(_response.text, True)
    
#########################################################################
# Load and validate configuration
#########################################################################
print_message("Loading and validating appliction configuration file...")

dir_path_app_home = os.path.dirname(os.path.realpath(__file__))
file_path_app_config = dir_path_app_home + "/config.json"
dir_path_raw_data = dir_path_app_home + "/raw/"
dir_path_output = dir_path_app_home + "/output/"

print_message("Home director:             " + dir_path_app_home, True)
print_message("Configuration file:        " + file_path_app_config, True)
print_message("Temporary data directory:  " + dir_path_raw_data, True)
print_message("Processed data directory:  " + dir_path_output, True)

config_app = {}
try:
    with open(file_path_app_config) as file_config:
        config_app = json.load(file_config)
except IOError:
    print_message("Cofniguration file \"" + file_path_app_config + "\" is not accessible.")
    quit()

if len(config_app) == 0: 
    print("No configuration item was found in the configuration file.")
    quit()

print_message("{n:,} configuration items were loaded.".format(n = len(config_app)), True)

host_address = config_app["couchbase"]["host"]
user_name = config_app["couchbase"]["user"]
password = config_app["couchbase"]["password"]
bucket_name = config_app["couchbase"]["bucket"]["name"]
bucket_definition_file = dir_path_app_home + "/" + config_app["couchbase"]["bucket"]["definitionFile"]
cbas_datasets_definition_file_path = dir_path_app_home + "/" + config_app["couchbase"]["cbasDatasetsDefinitionFile"]

#########################################################################
# Creating Couchbase bucket, GSI indexes and Analytics Service datasets
#########################################################################
if configure_couchbase:
    print_message("Creating Couchbase Server bucket...")
    create_bucket(host_address, user_name, password, bucket_definition_file)
    print_message("Creating Couchbase Server GSI indexes...")
    create_gsi_indexes(host_address, user_name, password, bucket_definition_file)
    print_message("Creating Couchbase Server Analytics Service datasets...")
    create_as_datasets(host_address, user_name, password, cbas_datasets_definition_file_path)

#########################################################################
# Download and process data from Google
#########################################################################
if download_dataset:
    print_message("Downloading and processing data from Google...")

    print("Cleaning temporary directory - " + dir_path_raw_data, True)
    prepare_directory(dir_path_raw_data)

    print("Cleaning output directory - " + dir_path_output, True)
    prepare_directory(dir_path_output)

    if len(config_app['dataFiles']) == 0: 
        print("No dataset was found in the configuration.")
    else:
        print_message("{n:,} datasets will be loaded.".format(n = len(config_app['dataFiles'])))
    
    for _dataset in config_app['dataFiles']:
        print("Processing dataset \"" + _dataset['type'] + "\"...", True)
        load_subregion = True if force_load_subregions else _dataset['loadSubregion']
        process_file(_dataset['type'], _dataset['url'], dir_path_raw_data, dir_path_output, load_subregion)

#########################################################################
# Loading the output files into Couchbase bucket
#########################################################################
if load_couchbase:
    print_message("Loading data files into Couchbase Server bucket", True)

    if (not host_address) or (not bucket_name) or (not user_name) or (not password):
        print_message("Couchbase Server connection information is incomplete.")
        print_message("Please check \"config.json\" file.")
        print_message("Data loading is aborted.")
        quit()
    
    print_message("Couchbase host address: " + host_address, True)
    print_message("Couchbase bucket name:  " + bucket_name, True)
    print_message("Couchbase user name:    " + user_name, True)

    print_message("Checking bucket \"" + bucket_name + "\" ...", True)
    if not check_bucket(host_address, user_name, password, bucket_name): 
        print_message("Data loading is aborted.")
        quit()
    
    print_message("Flushing bucket \"" + bucket_name + "\" ...", True)
    _api_endpoint = "http://" + host_address + ":8091/pools/default/buckets/" + bucket_name + "/controller/doFlush"
    print_message("Api endpoint to flush bucket:\r\n" + _api_endpoint, True)

    _response = requests.post(_api_endpoint, auth = (user_name, password))
    if _response.status_code == 200:
        print_message("Bucket \"" + bucket_name + "\" is flushed successfully.", True)
    else:
        print_message("Failed to flush bucket \"" + bucket_name + "\".")
        print_message(str(_response.status_code))
        print_message(_response.text)
        print_message("Data loading is aborted.")
        quit()

    _cbimport_cmd = "/opt/couchbase/bin/cbimport json --format lines -c http://{host_address}:8091 " \
            + " -u {user_name} -p {password} -d 'file://{data_file}' " \
            + " -b '{bucket_name}' -g {key_expression} -t 4"

    for _data_file in config_app['dataFiles']:
        _doc_type = _data_file['type']
        _doc_key = _data_file['key']
        _key_expression = _doc_type + "::" + _doc_key
        _file_path_data = dir_path_output + _doc_type + ".json"
        print_message("Loading dataset \"" + _file_path_data + "\" ...")
        
        _cmd = _cbimport_cmd.format(host_address = host_address, user_name = user_name, password = password, data_file = _file_path_data, \
                            bucket_name = bucket_name, key_expression = _key_expression, )
        print_message("cbimport command:\r\n" + _cmd, True)

        stream = os.popen(_cmd)
        _output = stream.read()
        print(_output)

