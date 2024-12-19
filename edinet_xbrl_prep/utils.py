


import pandera as pa
from pandera.typing import DataFrame, Series



from datetime import datetime, timedelta, date
from pydantic import BaseModel, Field
from time import sleep
import time
import contextlib

from typing import Literal
import json
from typing import Annotated
from pydantic.functional_validators import BeforeValidator



def get_columns_df(schima:pa.DataFrameModel)->list:
    return list(schima.to_schema().columns.keys())


def remove_empty_lists(lst):
    return [x for x in lst if x]

def flatten_list(lst):
    flat_list = []
    for item in lst:
        if isinstance(item, list):
            flat_list.extend(flatten_list(item))
        else:
            flat_list.append(item)
    return flat_list

def format_taxonomi(taxonomi_str:str)->str:
    """
    Convert
        From:
        jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
        To:
        jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF
    """
    return "_".join(taxonomi_str.split('_')[:-1])+":"+taxonomi_str.split('_')[-1]


@contextlib.contextmanager
def timer(name):
    t0=time.time()
    yield
    print(f'[{name}] done in {time.time()-t0:.2f} s ')

