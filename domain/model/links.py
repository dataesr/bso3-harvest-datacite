from typing import Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Links:
    self: str
    next: str

    @staticmethod
    def from_dict_custom(obj: Any) -> "Links":
        _self = str(obj.get("self"))
        _next = str(obj.get("next"))

        if obj.get("next") is not None:
            return Links(_self, _next)
        else:
            return Links(_self, "")
