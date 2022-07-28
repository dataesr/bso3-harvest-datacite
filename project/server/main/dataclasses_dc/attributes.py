from typing import List, Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from project.server.main.dataclasses_dc.attributes_childs import Creator, Title, Subject, Date, Types, RelatedIdentifier, RightsList, Description, GeoLocation


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
    def from_dict_custom(obj: Any) -> "Attributes":
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

        if obj.get("version") is not None:
            _version = str(obj.get("version"))
        else:
            _version = None

        _rightsList = [RightsList.from_dict_custom(y) for y in obj.get("rightsList")]
        _descriptions = [Description.from_dict_custom(y) for y in obj.get("descriptions")]
        _geoLocations = [GeoLocation.from_dict_custom(y) for y in obj.get("geoLocations")]
        _fundingReferences = [str(y) for y in obj.get("fundingReferences")]
        _url = str(obj.get("url"))

        if obj.get("contentUrl") is not None:
            _contentUrl = str(obj.get("contentUrl"))
        else:
            _contentUrl = None

        _metadataVersion = int(obj.get("metadataVersion"))
        _schemaVersion = str(obj.get("schemaVersion"))
        _source = str(obj.get("source"))
        _isActive = True
        _state = str(obj.get("state"))

        if obj.get("reason") is not None:
            _reason = str(obj.get("reason"))
        else:
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

        if obj.get("published") is not None:
            _published = int(obj.get("published"))
        else:
            _published = None

        _updated = str(obj.get("updated"))
        return Attributes(
            _doi,
            _identifiers,
            _creators,
            _titles,
            _publisher,
            _container,
            _publicationYear,
            _subjects,
            _contributors,
            _dates,
            _language,
            _types,
            _relatedIdentifiers,
            _sizes,
            _formats,
            _version,
            _rightsList,
            _descriptions,
            _geoLocations,
            _fundingReferences,
            _url,
            _contentUrl,
            _metadataVersion,
            _schemaVersion,
            _source,
            _isActive,
            _state,
            _reason,
            _viewCount,
            _downloadCount,
            _referenceCount,
            _citationCount,
            _partCount,
            _partOfCount,
            _versionCount,
            _versionOfCount,
            _created,
            _registered,
            _published,
            _updated,
        )
