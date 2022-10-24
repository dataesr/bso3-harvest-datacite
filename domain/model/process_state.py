from dataclasses import dataclass
from datetime import datetime


@dataclass
class ProcessState:
    id: int
    file_name: str
    file_path: str
    number_of_dois: int
    number_of_dois_with_null_attributes: int
    number_of_non_null_dois: int
    process_date: datetime
    processed: bool
