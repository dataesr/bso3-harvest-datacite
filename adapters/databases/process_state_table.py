from datetime import datetime
from domain.model.process_state import ProcessState
from sqlalchemy import Column, DateTime, Integer, String, Table, Boolean
from sqlalchemy.orm import registry
from sqlalchemy.engine import Engine

mapper_registry = registry()


@mapper_registry.mapped
class ProcessStateTable(ProcessState):
    __table__ = Table(
        "process_state",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("file_name", String(200)),
        Column("file_path", String(200)),
        Column("number_of_dois", Integer),
        Column("number_of_dois_with_null_attributes", Integer),
        Column("number_of_non_null_dois", Integer),
        Column("process_date", DateTime),
        Column("processed", Boolean),
    )
    id: int
    file_name: str
    file_path: str
    number_of_dois: int
    number_of_dois_with_null_attributes: int
    number_of_non_null_dois: int
    process_date: datetime
    processed: bool

    def __init__(
            self,
            process_date: datetime,
            file_name: str,
            file_path: str,
            number_of_dois: int = None,
            number_of_dois_with_null_attributes: int = None,
            number_of_non_null_dois: int = None,
            id: int = None,
            processed: bool = False,
    ):
        self.id = id
        self.file_name = file_name
        self.file_path = file_path
        self.number_of_dois = number_of_dois
        self.number_of_dois_with_null_attributes = number_of_dois_with_null_attributes
        self.number_of_non_null_dois = number_of_non_null_dois
        self.process_date = process_date
        self.processed = processed

    @staticmethod
    def createTable(engine: Engine):
        ProcessStateTable.__table__.create(engine, checkfirst=True)

    @staticmethod
    def checkExistence(engine: Engine):
        return ProcessStateTable.__table__.exists(engine)

    @staticmethod
    def dropTable(engine: Engine):
        ProcessStateTable.__table__.drop(engine, checkfirst=True)
