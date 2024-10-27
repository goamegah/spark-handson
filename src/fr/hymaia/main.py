import sys
from importlib import import_module
from src.fr.hymaia.utils.args_helper import parse_args

# TODO : import custom spark code dependencies

job_param = "job"
CITY_KEY = "city_key"
CLIENT_KEY = "client_key"

def main() -> None:
    args_dict = parse_args(sys.argv[1:])
    job = args_dict.pop(job_param)
    mod = import_module(job)
    fun = getattr(mod, "main")
    fun(**args_dict)

if __name__ == '__main__':
    main()