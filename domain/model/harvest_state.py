from dataclasses import dataclass
from datetime import datetime


@dataclass
class HarvestState:
    id: int
    number_missed: int
    number_slices: int
    start_date: datetime
    end_date: datetime
    status: str
    current_directory: str
    processed: bool
    slice_type: str
