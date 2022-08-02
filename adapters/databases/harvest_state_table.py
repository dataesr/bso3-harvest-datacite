from datetime import datetime
from domain.model.harvest_state import HarvestState
from sqlalchemy import Column, DateTime, Integer, String, Table
from sqlalchemy.orm import registry
from sqlalchemy.engine import Engine

mapper_registry = registry()


@mapper_registry.mapped
class HarvestStateTable(HarvestState):
    __table__ = Table(
        "harvest_state",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("number_missed", Integer),
        Column("start_date", DateTime),
        Column("end_date", DateTime),
        Column("status", String(50)),
        Column("current_directory", String(200)),
    )
    id: int
    number_missed: int
    start_date: datetime
    end_date: datetime
    status: str
    current_directory: str

    def __init__(self, number_missed: int, start_date: datetime, end_date: datetime, status: str, current_directory: str, id: int = None):
        self.number_missed = number_missed
        self.start_date = start_date
        self.end_date = end_date
        self.status = status
        self.current_directory = current_directory
        self.id = id

    @staticmethod
    def createTable(engine: Engine):
        HarvestStateTable.__table__.create(engine, checkfirst=True)

    def dropTable(engine: Engine):
        HarvestStateTable.__table__.drop(engine, checkfirst=True)
