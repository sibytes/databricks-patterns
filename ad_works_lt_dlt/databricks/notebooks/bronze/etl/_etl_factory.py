
from ._load import stream_load, batch_load
from ._load_audit import load_audit
from ._load_type import LoadType
from enum import Enum

class LoadFunction(Enum):
    load = "load"
    load_audit = "load_audit"
    load_ad_works_lt = "load_ad_works_lt"


_registered_autoload_fn = {
    "load": stream_load,
    "load_audit": load_audit
}

_registered_batch_fn = {
    "load": batch_load,
    "load_audit": load_audit
}


def get_load(load_function:LoadFunction, load_type:LoadType):

    if load_type == LoadType.autoloader:
        return _registered_autoload_fn[load_function.name]
    elif load_type == LoadType.batch:
        return _registered_batch_fn[load_function.name]