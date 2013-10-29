import os
import re
import simplejson as json


ROOT_DIR = '/export/brick1'


def debug(msg, level=1):
    """
    Outputs messages. More useful that print because it can be silenced.
    """
    #the level doesn't really matter, to be honest
    if level <= 2:
        print(msg)


def get_config():
    """
    read configuration data from json file

    first tries to read settings.json file from outside source directory and
    defaults to local_settings.json
    """
    config = {}
    try:
        json_file_path = os.path.join(ROOT_DIR, 'settings.json')
        with open(json_file_path, 'r') as config_file:
            config = json.load(config_file)
    except IOError:
        json_file_path = os.path.join(os.path.dirname(__file__), 'local_settings.json')
        with open(json_file_path, 'r') as config_file:
            config = json.load(config_file)
    return config


def get_filter_list():
    """ reads in a list of banned words from a json file and returns as a list """
    json_file_path = os.path.join(os.path.dirname(__file__), 'filter_list.json')
    filter_list = []
    with open(json_file_path, 'r') as filter_file:
        try:
            filter_list = json.load(filter_file)
        except json.JSONDecodeError as err:
            debug("ERROR reading json file: %s" % err)
    return filter_list


def filter_string(string, filter_list):
    """ iterate through list of banned words and replace for given string """
    for pattern, replacement in filter_list:
        string = re.sub(pattern, replacement, string)
    return string
