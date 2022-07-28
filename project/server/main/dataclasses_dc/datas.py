from typing import List, Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from project.server.main.dataclasses_dc.attributes import *
from  project.server.main.dataclasses_dc.relationships import *



@dataclass_json
@dataclass
class Doi:
    id: str
    type: str
    attributes: Attributes
    relationships: Relationships

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Doi':
        _id = str(obj.get("id"))
        _type = str(obj.get("type"))
        _attributes = Attributes.from_dict_custom(obj.get("attributes"))
        _relationships = Relationships.from_dict_custom(obj.get("relationships"))
        return Doi(_id, _type, _attributes, _relationships)