import json
import gzip

def hash_json(raw_data :dict, entry: int) -> int: 
    """
    Serialize and calculate the hash value of `raw_data["entry"]`

    Positional Arguments:
        raw_data: `dict` representing a JSON object.
        entry: the position of data in `raw_data`.
    
    Returns:
        An `int` representing the hash value of the serialized 
        `raw_data[entry]`. The serialized value is sorted by keys.
    """
    serialized_json = json.dumps(raw_data[entry], sort_keys=True)

    return hash(serialized_json)

def compress_json(data_json_str :str) -> bytes: 
    """
    Encode and compress the JSON string.
    
    Positional Arguments:
        data_json_str: A serialized JSON.

    Returns:
        A `Bytes` object containing the gzip-compressed serialized JSON. 
    """
    json_bytes = data_json_str.encode("utf-8")

    return gzip.compress(json_bytes)

def decompress_json(data_json_bytes :bytes) -> dict: 
    """
    Decompress and load the JSON object.
    
     Positional Arguments:
        data_json_bytes: gzip-compressed `Bytes` representing a serialzied
        JSON.

    Returns:
        A `dict` object representing the decompressed JSON.
    """
    json_string = gzip.decompress(data_json_bytes)
    json_obj = json.loads(json_string)
    
    return json_obj

