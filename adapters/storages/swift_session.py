from swiftclient.service import SwiftService
from domain.storages.abstract_swift_session import AbstractSwiftSession


class SwiftSession(AbstractSwiftSession):
    session: SwiftService

    def __init__(self, config: dict):
        self._session = SwiftService(options=config)

    def getSession(self) -> SwiftService:
        return self._session
