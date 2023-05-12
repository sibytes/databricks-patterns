
from ._load import _stream_load, _batch_load
from ._load_audit import _load_audit
from ._load_header_footer import _stream_load_header_footer, _batch_load_header_footer
from ._load_type import LoadType
from enum import Enum

class LoadFunction(Enum):
    load = "load"
    load_audit = "load_audit"
    load_header_footer = "load_header_footer"


_registered_autoload_fn = {
    "load": _stream_load,
    "load_audit": _load_audit,
    "load_header_footer": _stream_load_header_footer
}

_registered_batch_fn = {
    "load": _batch_load,
    "load_audit": _load_audit,
    "load_header_footer": _batch_load_header_footer
}


def get_load(load_function:LoadFunction, load_type:LoadType):

    if load_type == LoadType.autoloader:
        return _registered_autoload_fn[load_function.name]
    elif load_type == LoadType.batch:
        return _registered_batch_fn[load_function.name]