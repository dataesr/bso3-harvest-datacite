from typing import List, Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from  project.server.main.dataclasses_dc.meta_childs import *



@dataclass_json
@dataclass
class Meta:
    total: int
    totalPages: int
    page: int
    states: List[State]
    resourceTypes: List[ResourceType]
    created: List[Created]
    published: List[Published]
    registered: List[Registered]
    providers: List[Provider]
    clients: List[Clients]
    affiliations: List[object]
    prefixes: List[Prefix]
    certificates: List[object]
    licenses: List[License]
    schemaVersions: List[SchemaVersion]
    linkChecksStatus: List[object]
    subjects: List[Subjects]
    fieldsOfScience: List[FieldsOfScience]
    citations: List[object]
    views: List[object]
    downloads: List[object]

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Meta':
        _total = int(obj.get("total"))
        _totalPages = int(obj.get("totalPages"))
        if (obj.get("page") != None) : 
            _page = int(obj.get("page"))
        else : 
            _page = None

        _states = [State.from_dict_custom(y) for y in obj.get("states")]
        _resourceTypes = [ResourceType.from_dict_custom(y) for y in obj.get("resourceTypes")]
        _created = [Created.from_dict_custom(y) for y in obj.get("created")]
        _published = [Published.from_dict_custom(y) for y in obj.get("published")]
        _registered = [Registered.from_dict_custom(y) for y in obj.get("registered")]
        _providers = [Provider.from_dict_custom(y) for y in obj.get("providers")]
        _clients = [Clients.from_dict_custom(y) for y in obj.get("clients")]
        _affiliations = [str(y) for y in obj.get("affiliations")]
        _prefixes = [Prefix.from_dict_custom(y) for y in obj.get("prefixes")]
        _certificates = [str(y) for y in obj.get("certificates")]
        _licenses = [License.from_dict_custom(y) for y in obj.get("licenses")]
        _schemaVersions = [SchemaVersion.from_dict_custom(y) for y in obj.get("schemaVersions")]
        _linkChecksStatus = [str(y) for y in obj.get("linkChecksStatus")]
        _subjects = [Subjects.from_dict_custom(y) for y in obj.get("subjects")]
        _fieldsOfScience = [FieldsOfScience.from_dict_custom(y) for y in obj.get("fieldsOfScience")]
        _citations = [str(y) for y in obj.get("citations")]
        _views = [str(y) for y in obj.get("views")]
        _downloads = [str(y) for y in obj.get("downloads")]
        return Meta(_total, _totalPages, _page, _states, _resourceTypes, _created, _published, _registered, _providers, _clients, _affiliations, _prefixes, _certificates, _licenses, _schemaVersions, _linkChecksStatus, _subjects, _fieldsOfScience, _citations, _views, _downloads)
