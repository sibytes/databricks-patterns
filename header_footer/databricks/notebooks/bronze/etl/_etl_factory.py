
from ._load import stream_load, batch_load
from ._load_audit import load_audit
from ._load_header_footer import stream_load_header_footer, batch_load_header_footer
from ._load_type import LoadType
from enum import Enum

class LoadFunction(Enum):
    load = "load"
    load_audit = "load_audit"
    load_header_footer = "load_header_footer"


_registered_autoload_fn = {
    "load": stream_load,
    "load_audit": load_audit,
    "load_header_footer": stream_load_header_footer
}

_registered_batch_fn = {
    "load": batch_load,
    "load_audit": load_audit,
    "load_header_footer": batch_load_header_footer
}


def get_load(load_function:LoadFunction, load_type:LoadType):

    if load_type == LoadType.autoloader:
        return _registered_autoload_fn[load_function.name]
    elif load_type == LoadType.batch:
        return _registered_batch_fn[load_function.name]