from typing import Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Clients:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Clients":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Clients(_id, _title, _count)


@dataclass_json
@dataclass
class Created:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Created":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Created(_id, _title, _count)


@dataclass_json
@dataclass
class FieldsOfScience:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "FieldsOfScience":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return FieldsOfScience(_id, _title, _count)


@dataclass_json
@dataclass
class License:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "License":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return License(_id, _title, _count)


@dataclass_json
@dataclass
class Prefix:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Prefix":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Prefix(_id, _title, _count)


@dataclass_json
@dataclass
class Provider:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Provider":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Provider(_id, _title, _count)


@dataclass_json
@dataclass
class Published:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Published":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Published(_id, _title, _count)


@dataclass_json
@dataclass
class Registered:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Registered":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Registered(_id, _title, _count)


@dataclass_json
@dataclass
class ResourceType:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "ResourceType":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return ResourceType(_id, _title, _count)


@dataclass_json
@dataclass
class SchemaVersion:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "SchemaVersion":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return SchemaVersion(_id, _title, _count)


@dataclass_json
@dataclass
class State:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "State":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return State(_id, _title, _count)


@dataclass_json
@dataclass
class Subjects:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> "Subjects":
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Subjects(_id, _title, _count)
