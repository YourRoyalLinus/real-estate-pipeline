import json
import gzip

def hash_json(raw_data :dict, entry: int) -> int: 
    serialized_json = json.dumps(raw_data[entry], sort_keys=True)

    return hash(serialized_json)

def compress_json(data_json_str :str) -> bytes: 
    json_bytes = data_json_str.encode("utf-8")

    return gzip.compress(json_bytes)

def decompress_json(data_json_bytes :bytes) -> dict: 
    json_string = gzip.decompress(data_json_bytes)
    json_obj = json.loads(json_string)
    
    return json_obj

