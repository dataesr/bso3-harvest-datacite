from typing import Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from project.server.main.dataclasses_dc.attributes import Attributes
from project.server.main.dataclasses_dc.relationships import Relationships


@dataclass_json
@dataclass
class Datas:
    id: str
    type: str
    attributes: Attributes
    relationships: Relationships

    @staticmethod
    def from_dict_custom(obj: Any) -> "Datas":
        _id = str(obj.get("id"))
        _type = str(obj.get("type"))
        _attributes = Attributes.from_dict_custom(obj.get("attributes"))
        _relationships = Relationships.from_dict_custom(obj.get("relationships"))
        return Datas(_id, _type, _attributes, _relationships)
