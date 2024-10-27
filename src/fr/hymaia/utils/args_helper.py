from typing import List, Dict, Any 

def parse_args(args: List[str]) -> Dict[str, Any]:
    return dict(args.replace("--", "").split("=") for args in args)

def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    return {**dict1, **dict2}