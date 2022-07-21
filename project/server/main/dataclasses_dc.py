from typing import List
from typing import Any, Tuple, Optional
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
import json


@dataclass_json
@dataclass
class Clients:
    id: str
    title: str
    count: int

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Clients':
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
    def from_dict_custom(obj: Any) -> 'Created':
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
    def from_dict_custom(obj: Any) -> 'FieldsOfScience':
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
    def from_dict_custom(obj: Any) -> 'License':
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
    def from_dict_custom(obj: Any) -> 'Prefix':
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
    def from_dict_custom(obj: Any) -> 'Provider':
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
    def from_dict_custom(obj: Any) -> 'Published':
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
    def from_dict_custom(obj: Any) -> 'Registered':
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
    def from_dict_custom(obj: Any) -> 'ResourceType':
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
    def from_dict_custom(obj: Any) -> 'SchemaVersion':
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
    def from_dict_custom(obj: Any) -> 'State':
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
    def from_dict_custom(obj: Any) -> 'Subjects':
        _id = str(obj.get("id"))
        _title = str(obj.get("title"))
        _count = int(obj.get("count"))
        return Subjects(_id, _title, _count)

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


@dataclass_json
@dataclass
class Links:
    self: str
    next: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Links':
        _self = str(obj.get("self"))
        _next = str(obj.get("next"))
        
        if obj.get("next")!=None :
            return Links(_self, _next)
        else:
            return Links(_self,'')


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


@dataclass_json
@dataclass
class Creator:
    name: str
    givenName: str
    familyName: str
    affiliation: List[object]
    nameIdentifiers: List[object]


    @staticmethod
    def from_dict_custom(obj: Any) -> 'Creator':
        _name = str(obj.get("name"))
        _givenName = str(obj.get("givenName",""))
        _familyName = str(obj.get("familyName"))
        _affiliation = [str(y) for y in obj.get("affiliation")]
        _nameIdentifiers = [str(y) for y in obj.get("nameIdentifiers")]
        return Creator(_name, _givenName, _familyName, _affiliation, _nameIdentifiers)


@dataclass_json
@dataclass
class Date:
    date: object
    dateType: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Date':
        _date = str(obj.get("date"))
        _dateType = str(obj.get("dateType"))
        return Date(_date, _dateType)


@dataclass_json
@dataclass
class Description:
    description: str
    descriptionType: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Description':
        _description = str(obj.get("description"))
        _descriptionType = str(obj.get("descriptionType"))
        return Description(_description, _descriptionType)


@dataclass_json
@dataclass
class GeoLocationPoint:
    pointLatitude: str
    pointLongitude: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'GeoLocationPoint':
        _pointLatitude = str(obj.get("pointLatitude"))
        _pointLongitude = str(obj.get("pointLongitude"))
        return GeoLocationPoint(_pointLatitude, _pointLongitude)


@dataclass_json
@dataclass
class GeoLocation:
    geoLocationPoint: Optional[Tuple[int]]=None
    geoLocationPlace: Optional[str]=None

    @staticmethod
    def from_dict_custom(obj: Any) -> 'GeoLocation':
        
        if obj.get("geoLocationPoint")!=None :
            _geoLocationPoint = GeoLocationPoint.from_dict_custom(obj.get("geoLocationPoint"))
            return GeoLocation(_geoLocationPoint, "")
        else:
            _geoLocationPlace = str(obj.get("geoLocationPlace"))
            return GeoLocation(GeoLocationPoint("",""), _geoLocationPlace)


@dataclass_json
@dataclass
class RelatedIdentifier:
    relationType: str
    relatedIdentifier: str
    relatedIdentifierType: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'RelatedIdentifier':
        _relationType = str(obj.get("relationType"))
        _relatedIdentifier = str(obj.get("relatedIdentifier"))
        _relatedIdentifierType = str(obj.get("relatedIdentifierType"))
        return RelatedIdentifier(_relationType, _relatedIdentifier, _relatedIdentifierType)




@dataclass_json
@dataclass
class RightsList:
    rights: str
    rightsUri: str
    schemeUri: str
    rightsIdentifier: str
    rightsIdentifierScheme: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'RightsList':
        _rights = str(obj.get("rights"))
        _rightsUri = str(obj.get("rightsUri"))
        _schemeUri = str(obj.get("schemeUri"))
        _rightsIdentifier = str(obj.get("rightsIdentifier"))
        _rightsIdentifierScheme = str(obj.get("rightsIdentifierScheme"))
        return RightsList(_rights, _rightsUri, _schemeUri, _rightsIdentifier, _rightsIdentifierScheme)


@dataclass_json
@dataclass
class Title:
    title: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Title':
        _title = str(obj.get("title"))
        return Title(_title)


@dataclass_json
@dataclass
class Subject:
    subject: str
    subjectScheme: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Subject':
        _subject = str(obj.get("subject"))
        _subjectScheme = str(obj.get("subjectScheme"))
        return Subject(_subject, _subjectScheme)


@dataclass_json
@dataclass
class Types:
    ris: str
    bibtex: str
    citeproc: str
    schemaOrg: str
    resourceType: str
    resourceTypeGeneral: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Types':
        _ris = str(obj.get("ris"))
        _bibtex = str(obj.get("bibtex"))
        _citeproc = str(obj.get("citeproc"))
        _schemaOrg = str(obj.get("schemaOrg"))
        _resourceType = str(obj.get("resourceType"))
        _resourceTypeGeneral = str(obj.get("resourceTypeGeneral"))
        return Types(_ris, _bibtex, _citeproc, _schemaOrg, _resourceType, _resourceTypeGeneral)



@dataclass_json
@dataclass
class Attributes:
    doi: str
    identifiers: List[object]
    creators: List[Creator]
    titles: List[Title]
    publisher: str
    container: str
    publicationYear: int
    subjects: List[Subject]
    contributors: List[object]
    dates: List[Date]
    language: str
    types: Types
    relatedIdentifiers: List[RelatedIdentifier]
    sizes: List[str]
    formats: List[str]
    version: str
    rightsList: List[RightsList]
    descriptions: List[Description]
    geoLocations: List[GeoLocation]
    fundingReferences: List[object]
    url: str
    contentUrl: str
    metadataVersion: int
    schemaVersion: str
    source: str
    isActive: bool
    state: str
    reason: str
    viewCount: int
    downloadCount: int
    referenceCount: int
    citationCount: int
    partCount: int
    partOfCount: int
    versionCount: int
    versionOfCount: int
    created: str
    registered: str
    published: int
    updated: str

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Attributes':
        _doi = str(obj.get("doi"))
        _identifiers = [str(y) for y in obj.get("identifiers")]
        _creators = [Creator.from_dict_custom(y) for y in obj.get("creators")]
        _titles = [Title.from_dict_custom(y) for y in obj.get("titles")]
        _publisher = str(obj.get("publisher"))
        _container = str(obj.get("container"))
        _publicationYear = int(obj.get("publicationYear"))
        _subjects = [Subject.from_dict_custom(y) for y in obj.get("subjects")]
        _contributors = [str(y) for y in obj.get("contributors")]
        _dates = [Date.from_dict_custom(y) for y in obj.get("dates")]
        _language = str(obj.get("language"))
        _types = Types.from_dict_custom(obj.get("types"))
        _relatedIdentifiers = [RelatedIdentifier.from_dict_custom(y) for y in obj.get("relatedIdentifiers")]
        _sizes = [str(y) for y in obj.get("sizes")]
        _formats = [str(y) for y in obj.get("formats")]

        if (obj.get("version") != None) : 
            _version = str(obj.get("version"))
        else : 
            _version = None

        _rightsList = [RightsList.from_dict_custom(y) for y in obj.get("rightsList")]
        _descriptions = [Description.from_dict_custom(y) for y in obj.get("descriptions")]
        _geoLocations = [GeoLocation.from_dict_custom(y) for y in obj.get("geoLocations")]
        _fundingReferences = [str(y) for y in obj.get("fundingReferences")]
        _url = str(obj.get("url"))

        if (obj.get("contentUrl") != None) : 
            _contentUrl = str(obj.get("contentUrl"))
        else : 
            _contentUrl = None
        
        _metadataVersion = int(obj.get("metadataVersion"))
        _schemaVersion = str(obj.get("schemaVersion"))
        _source = str(obj.get("source"))
        _isActive = True
        _state = str(obj.get("state"))

        if (obj.get("reason") != None) : 
            _reason = str(obj.get("reason"))
        else : 
            _reason = None

        _viewCount = int(obj.get("viewCount"))
        _downloadCount = int(obj.get("downloadCount"))
        _referenceCount = int(obj.get("referenceCount"))
        _citationCount = int(obj.get("citationCount"))
        _partCount = int(obj.get("partCount"))
        _partOfCount = int(obj.get("partOfCount"))
        _versionCount = int(obj.get("versionCount"))
        _versionOfCount = int(obj.get("versionOfCount"))
        _created = str(obj.get("created"))
        _registered = str(obj.get("registered"))

        if (obj.get("published") != None) : 
            _published = int(obj.get("published"))
        else : 
            _published = None

        _updated = str(obj.get("updated"))
        return Attributes(_doi, _identifiers, _creators, _titles, _publisher, _container, _publicationYear, _subjects, _contributors, _dates, _language, _types, _relatedIdentifiers, _sizes, _formats, _version, _rightsList, _descriptions, _geoLocations, _fundingReferences, _url, _contentUrl, _metadataVersion, _schemaVersion, _source, _isActive, _state, _reason, _viewCount, _downloadCount, _referenceCount, _citationCount, _partCount, _partOfCount, _versionCount, _versionOfCount, _created, _registered, _published, _updated)


@dataclass_json
@dataclass
class Datas:
    id: str
    type: str
    attributes: Attributes
    relationships: Relationships

    @staticmethod
    def from_dict_custom(obj: Any) -> 'Datas':
        _id = str(obj.get("id"))
        _type = str(obj.get("type"))
        _attributes = Attributes.from_dict_custom(obj.get("attributes"))
        _relationships = Relationships.from_dict_custom(obj.get("relationships"))
        return Datas(_id, _type, _attributes, _relationships)

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
