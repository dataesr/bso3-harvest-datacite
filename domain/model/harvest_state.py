from dataclasses import dataclass
from datetime import datetime

@dataclass
class HarvestState():
    id: int
    number_missed: int
    date_debut: datetime
    status: str