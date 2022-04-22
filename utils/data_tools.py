import json
from pandas.io.json._normalize import nested_to_record


def flatten(dictionary):
    return nested_to_record(dictionary, sep="_")


def dict_parser(instance):
    for k, v in instance.items():
        try:
            out_instance = json.loads(v)
            if isinstance(out_instance, dict):
                return dict_parser(out_instance)
            instance[k] = out_instance
        except (json.JSONDecodeError, TypeError):
            pass
    return instance
