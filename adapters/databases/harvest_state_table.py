from datetime import date, datetime
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
        Column("date_debut", DateTime),
        Column("status", String(50)),
    )
    id: int
    number_missed: int
    date_debut: datetime
    status: str

    def __init__(self, number_missed: int, date_debut: datetime, status: str, id: int = None):
        self.number_missed = number_missed
        self.date_debut = date_debut
        self.status = status
        self.id = id

    @staticmethod
    def createTable(engine: Engine):
        HarvestStateTable.__table__.create(engine, checkfirst=True)
