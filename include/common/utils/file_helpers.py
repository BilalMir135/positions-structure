import json

def load_json_file(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    return data