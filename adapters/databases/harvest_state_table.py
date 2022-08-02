from datetime import datetime
from domain.model.harvest_state import HarvestState
from sqlalchemy import Column, DateTime, Integer, String, Table, Boolean
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
        Column("number_slices", Integer),
        Column("start_date", DateTime),
        Column("end_date", DateTime),
        Column("status", String(50)),
        Column("current_directory", String(200)),
        Column("processed", Boolean),
    )
    id: int
    number_missed: int
    number_slices: int
    start_date: datetime
    end_date: datetime
    status: str
    current_directory: str
    processed: bool

    def __init__(self, start_date: datetime, end_date: datetime, status: str, current_directory: str, id: int = None, number_missed: int = None, number_slices: int = None, processed: bool = False):
        self.number_missed = number_missed
        self.number_slices = number_slices
        self.start_date = start_date
        self.end_date = end_date
        self.status = status
        self.current_directory = current_directory
        self.processed = processed
        self.id = id

    @staticmethod
    def createTable(engine: Engine):
        HarvestStateTable.__table__.create(engine, checkfirst=True)

    def dropTable(engine: Engine):
        HarvestStateTable.__table__.drop(engine, checkfirst=True)
