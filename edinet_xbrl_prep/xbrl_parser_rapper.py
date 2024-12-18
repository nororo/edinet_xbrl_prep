
import pandas as pd

from arelle import Cntlr
from arelle.ModelValue import qname

from zipfile import ZipFile
import json
from pathlib import Path
from typing import List, Tuple, Union
import pandera as pa
from pandera.typing import DataFrame, Series

from typing import Literal
import json
from typing import Annotated
from pydantic import BaseModel, Field

from pydantic.functional_validators import BeforeValidator
from .utils import *
# %% #################################################################
#
#            schima
#
######################################################################

class xbrl_elm_schima(pa.DataFrameModel):
    """
        key:prefix+":"+element_name
        data_str
        context_ref
    """
    key: Series[str] = pa.Field(nullable=True)
    data_str: Series[str] = pa.Field(nullable=True)
    context_ref: Series[str] = pa.Field(nullable=True)
    decimals: Series[str] = pa.Field(nullable=True)# T:-3, M:-6, B:-9
    precision: Series[str] = pa.Field(nullable=True)
    element_name: Series[str] = pa.Field(nullable=True)
    unit: Series[str] = pa.Field(nullable=True)# 'JPY'
    period_type: Series[str] = pa.Field(isin=['instant','duration'],nullable=True) # 'instant','duration'
    isTextBlock_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    abstract_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    period_start: Series[str] = pa.Field(nullable=True)
    period_end: Series[str] = pa.Field(nullable=True)
    instant_date: Series[str] = pa.Field(nullable=True)

StrOrNone = Annotated[str, BeforeValidator(lambda x: x or "")]

class ArreleFact(BaseModel):

    key:StrOrNone
    data_str:StrOrNone
    decimals:StrOrNone
    precision:StrOrNone
    context_ref:StrOrNone
    element_name:StrOrNone
    unit:StrOrNone
    period_type:StrOrNone
    isTextBlock_flg:StrOrNone
    abstract_flg:StrOrNone
    period_start:StrOrNone
    period_end:StrOrNone
    instant_date:StrOrNone
    end_date_pv:StrOrNone
    instant_date_pv:StrOrNone
    scenario:StrOrNone

def get_fact_data(fact)->ArreleFact:
    fact_data = {
        'key':str(fact.qname),
        'data_str':fact.value,
        'decimals':fact.decimals,
        'precision':fact.precision,
        'context_ref':fact.contextID,
        'element_name':str(fact.qname.localName),
        'unit':fact.unitID,#(str) – unitRef attribute
        'period_type':fact.concept.periodType,#'instant','duration'
        'isTextBlock_flg':int(fact.concept.isTextBlock), # 0,1
        'abstract_flg':int(fact.concept.abstract=='true'), # Note: fatc.concept.abstract is str not bool.
    }
    if fact.context.startDatetime:
        fact_data['period_start'] = fact.context.startDatetime.strftime('%Y-%m-%d')
    else:
        fact_data['period_start'] = None
    if fact.context.endDatetime:
        fact_data['period_end'] = fact.context.endDatetime.strftime('%Y-%m-%d') # 1 day added???
    else:
        fact_data['period_end'] = None
    if fact.context.instantDatetime:
        fact_data['instant_date'] = fact.context.instantDatetime.strftime('%Y-%m-%d') # 1 day added???
    else:
        fact_data['instant_date'] = None

    
    fact_data['end_date_pv']=None
    fact_data['instant_date_pv']=None
    for item in fact.context.propertyView:
                if item:
                    if item[0] == 'endDate':
                        fact_data['end_date_pv'] = item[1]
                    elif item[0] == 'instant':
                        fact_data['instant_date_pv'] = item[1]
    scenario = []
    for (dimension, dim_value) in fact.context.scenDimValues.items():
        scenario.append({
            'ja': (
                dimension.label(preferredLabel=None, lang='ja', linkroleHint=None),
                dim_value.member.label(preferredLabel=None, lang='ja', linkroleHint=None)),
            'en': (
                dimension.label(preferredLabel=None, lang='en', linkroleHint=None),
                dim_value.member.label(preferredLabel=None, lang='en', linkroleHint=None)),
            'id': (
                dimension.id,
                dim_value.member.id),
        })
    if scenario:
            scenario_json = json.dumps(
                scenario, ensure_ascii=False, separators=(',', ':'))
    else:
        scenario_json = None

    fact_data['scenario'] = scenario_json
    return fact_data


def get_xbrl_dei_df(xbrl_filename:str,log_dict,temp_dir)->(xbrl_elm_schima,dict):
    if log_dict['arelle_log_fname'] is None:
        log_dict['arelle_log_fname'] = str(temp_dir / "arelle.log")

    ctrl = Cntlr.Cntlr(logFileName=str(log_dict['arelle_log_fname']))
    model_xbrl = ctrl.modelManager.load(xbrl_filename)
    localname="AccountingStandardsDEI"
    qname_prefix = "jpdei_cor"
    ns = model_xbrl.prefixedNamespaces[qname_prefix]
    
    facts=model_xbrl.factsByQname[qname(ns, name=f"{qname_prefix}:{localname}")]
    fact_list = list(facts)
    if len(fact_list)>0:
        log_dict[localname]=fact_list[0].value
    else:
        log_dict[localname]=None
    ctrl.close()
    return log_dict


def get_xbrl_df(xbrl_filename:str,log_dict,temp_dir)->(xbrl_elm_schima,dict):
    """
    arelle.ModelInstanceObject - Arelle
        https://arelle.readthedocs.io/en/2.18.0/apidocs/arelle/arelle.ModelInstanceObject.html#arelle.ModelInstanceObject.ModelFact
    """
    if log_dict['arelle_log_fname'] is None:
        log_dict['arelle_log_fname'] = str(temp_dir / "arelle.log")

    ctrl = Cntlr.Cntlr(logFileName=str(log_dict['arelle_log_fname']))
    model_xbrl = ctrl.modelManager.load(xbrl_filename)
    if len(model_xbrl.facts)==0:
        log_dict['xbrl_load_status']="Failure"
        ctrl.close()
        return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)),log_dict
    else:
        log_dict['xbrl_load_status']="Success"
        fact_dict_list = []
        for fact in model_xbrl.facts:
            fact_dict_list.append(get_fact_data(fact))
        # log
        ctrl.close()
        return pd.DataFrame(fact_dict_list).drop_duplicates(),log_dict

def get_xbrl_rapper(docid,zip_file,temp_dir,out_path,update_flg=False,log_dict=None):
    if log_dict is None:
        log_dict = {"is_xbrl_file":None, "is_xsd_file":None, "arelle_log_fname":None,"status":None,"error_message":None}
    
    
    try:
        already_exist_flg=(out_path / "xbrl_parsed.csv").exists()
        if (already_exist_flg)&(update_flg==False):
            log_dict["already_parse_xbrl"] = True
            xbrl_parsed=pd.read_csv(out_path / "xbrl_parsed.csv",dtype=dtype(xbrl_elm_schima))
            return xbrl_elm_schima(xbrl_parsed),log_dict
    except Exception as e:
        already_exist_flg=False
        pass
    try:
        log_dict["already_parse_xbrl"] = False
        #data_dir_raw=PROJDIR / "data" / "1_raw"
        #zip_file = list(data_dir_raw.glob("data_pool_*/"+docid+".zip"))[0]
        with ZipFile(str(zip_file)) as zf:
            fn=[item for item in zf.namelist() if (".xbrl" in item)&("PublicDoc" in item)&("asr" in item)]
            if len(fn)>0:
                zf.extract(fn[0], out_path)
                log_dict["is_xbrl_file"] = True
            else:
                log_dict["is_xbrl_file"] = False
            fn=[item for item in zf.namelist() if (".xsd" in item)&("PublicDoc" in item)&("asr" in item)]
            if len(fn)>0:
                zf.extract(fn[0], out_path)
                log_dict["is_xsd_file"] = True
            else:
                log_dict["is_xsd_file"] = False
        xbrl_path=out_path / "XBRL" / "PublicDoc"
        if (len(list(xbrl_path.glob("*.xbrl")))>0)&(len(list(xbrl_path.glob("*.xsd")))>0): # xbrl and xsd file exists
            xbrl_filename=str(list(xbrl_path.glob("*.xbrl"))[0])
            if (not already_exist_flg)|(update_flg==True):
                (xbrl_path / "arelle.log").touch()
                xbrl_parsed,log_dict=get_xbrl_df(xbrl_filename,log_dict,temp_dir)
                xbrl_parsed.to_csv(out_path / "xbrl_parsed.csv",index=False)
            
                log_dict=get_xbrl_dei_df(xbrl_filename,log_dict,temp_dir)
                log_dict["get_xbrl_status"] = "Success"
                log_dict["get_xbrl_error_message"] = None

            else:
                log_dict["get_xbrl_status"] = "Success"
                log_dict["get_xbrl_error_message"] = None
            out_filename=str(xbrl_path / "log_dict.json")
            with open(out_filename, mode="wt", encoding="utf-8") as f:
                json.dump(log_dict, f, ensure_ascii=False, indent=2)
            
            return xbrl_parsed,log_dict
        else:
            log_dict["get_xbrl_status"] = "Failure"
            log_dict["get_xbrl_error_message"] = "No xbrl or xsd file"
            out_filename=str(xbrl_path / "log_dict.json")
            with open(out_filename, mode="wt", encoding="utf-8") as f:
                json.dump(log_dict, f, ensure_ascii=False, indent=2)
            return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)),log_dict
    except Exception as e:
        log_dict["get_xbrl_status"] = "Failure"
        log_dict["get_xbrl_error_message"] = e
        out_filename=str(xbrl_path / "log_dict.json")
        with open(out_filename, mode="wt", encoding="utf-8") as f:
            json.dump(log_dict, f, ensure_ascii=False, indent=2)
            
        return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)),log_dict