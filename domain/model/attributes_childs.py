from typing import List, Any, Tuple, Optional
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Creator:
    name: str
    givenName: str
    familyName: str
    affiliation: List[object]
    nameIdentifiers: List[object]

    @staticmethod
    def from_dict_custom(obj: Any) -> "Creator":
        _name = str(obj.get("name"))
        _givenName = str(obj.get("givenName", ""))
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
    def from_dict_custom(obj: Any) -> "Date":
        _date = str(obj.get("date"))
        _dateType = str(obj.get("dateType"))
        return Date(_date, _dateType)


@dataclass_json
@dataclass
class Description:
    description: str
    descriptionType: str

    @staticmethod
    def from_dict_custom(obj: Any) -> "Description":
        _description = str(obj.get("description"))
        _descriptionType = str(obj.get("descriptionType"))
        return Description(_description, _descriptionType)


@dataclass_json
@dataclass
class GeoLocationPoint:
    pointLatitude: str
    pointLongitude: str

    @staticmethod
    def from_dict_custom(obj: Any) -> "GeoLocationPoint":
        _pointLatitude = str(obj.get("pointLatitude"))
        _pointLongitude = str(obj.get("pointLongitude"))
        return GeoLocationPoint(_pointLatitude, _pointLongitude)


@dataclass_json
@dataclass
class GeoLocation:
    geoLocationPoint: Optional[Tuple[int]] = None
    geoLocationPlace: Optional[str] = None

    @staticmethod
    def from_dict_custom(obj: Any) -> "GeoLocation":

        if obj.get("geoLocationPoint") is not None:
            _geoLocationPoint = GeoLocationPoint.from_dict_custom(obj.get("geoLocationPoint"))
            return GeoLocation(_geoLocationPoint, "")
        else:
            _geoLocationPlace = str(obj.get("geoLocationPlace"))
            return GeoLocation(GeoLocationPoint("", ""), _geoLocationPlace)


@dataclass_json
@dataclass
class RelatedIdentifier:
    relationType: str
    relatedIdentifier: str
    relatedIdentifierType: str

    @staticmethod
    def from_dict_custom(obj: Any) -> "RelatedIdentifier":
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
    def from_dict_custom(obj: Any) -> "RightsList":
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
    def from_dict_custom(obj: Any) -> "Title":
        _title = str(obj.get("title"))
        return Title(_title)


@dataclass_json
@dataclass
class Subject:
    subject: str
    subjectScheme: str

    @staticmethod
    def from_dict_custom(obj: Any) -> "Subject":
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
    def from_dict_custom(obj: Any) -> "Types":
        _ris = str(obj.get("ris"))
        _bibtex = str(obj.get("bibtex"))
        _citeproc = str(obj.get("citeproc"))
        _schemaOrg = str(obj.get("schemaOrg"))
        _resourceType = str(obj.get("resourceType"))
        _resourceTypeGeneral = str(obj.get("resourceTypeGeneral"))
        return Types(_ris, _bibtex, _citeproc, _schemaOrg, _resourceType, _resourceTypeGeneral)
