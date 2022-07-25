from typing import Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from project.server.main.dataclasses_dc.datas import *
from project.server.main.dataclasses_dc.meta import *
from project.server.main.dataclasses_dc.links import *


@dataclass_json
@dataclass
class Root:
    data: Datas
    meta: Meta
    links: Links

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Root':
        _data =[Datas.from_dict_custom(y) for y in obj.get("data")]
        _meta = Meta.from_dict_custom(obj.get("meta"))
        _links = Links.from_dict_custom(obj.get("links"))

        return Root(_data, _meta, _links)