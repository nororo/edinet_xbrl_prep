


import pandera as pa
from pandera.typing import DataFrame, Series



from datetime import datetime, timedelta, date
from pydantic import BaseModel, Field
from time import sleep
from typing import Literal
import json
from typing import Annotated
from pydantic.functional_validators import BeforeValidator



def get_columns_df(schima:pa.DataFrameModel)->list:
    return list(schima.to_schema().columns.keys())
