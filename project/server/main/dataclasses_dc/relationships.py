from typing import List, Any, Tuple, Optional
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Data:
    id: str
    type: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Data':
        _id = str(obj.get("id"))
        _type = str(obj.get("type"))
        return Data(_id, _type)



@dataclass_json
@dataclass
class Client:
    data: Data

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Client':
        _data = Data.from_dict_custom(obj.get("data"))
        return Client(_data)


@dataclass_json
@dataclass
class Relationships:
    client: Client

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Relationships':
        _client = Client.from_dict_custom(obj.get("client"))
        return Relationships(_client)