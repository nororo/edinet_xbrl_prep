import json
from pathlib import Path
from typing import Annotated
from zipfile import ZipFile

import pandas as pd
import pandera as pa
from arelle import Cntlr
from arelle.ModelValue import qname
from pandera.typing import Series
from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from .utils import *


# %% #################################################################
#
#            schima
#
######################################################################
def dtype(df_type, use_nullable=True):
    dc = {}
    schema = df_type.to_schema()
    for name, column in schema.columns.items():
        typ = column.dtype.type
        if use_nullable and column.nullable and column.dtype.type == int:
            typ = "Int64"
        dc[name] = typ
    return dc


class xbrl_elm_schima(pa.DataFrameModel):
    """key:prefix+":"+element_name
    data_str
    context_ref
    """

    key: Series[str] = pa.Field(nullable=True)
    data_str: Series[str] = pa.Field(nullable=True)
    context_ref: Series[str] = pa.Field(nullable=True)
    decimals: Series[str] = pa.Field(nullable=True)  # T:-3, M:-6, B:-9
    precision: Series[str] = pa.Field(nullable=True)
    element_name: Series[str] = pa.Field(nullable=True)
    unit: Series[str] = pa.Field(nullable=True)  # 'JPY'
    period_type: Series[str] = pa.Field(
        isin=["instant", "duration"], nullable=True
    )  # 'instant','duration'
    isTextBlock_flg: Series[int] = pa.Field(isin=[0, 1], nullable=True)  # 0,1
    abstract_flg: Series[int] = pa.Field(isin=[0, 1], nullable=True)  # 0,1
    period_start: Series[str] = pa.Field(nullable=True)
    period_end: Series[str] = pa.Field(nullable=True)
    instant_date: Series[str] = pa.Field(nullable=True)


StrOrNone = Annotated[str, BeforeValidator(lambda x: x or "")]


class ArreleFact(BaseModel):
    key: StrOrNone
    data_str: StrOrNone
    decimals: StrOrNone
    precision: StrOrNone
    context_ref: StrOrNone
    element_name: StrOrNone
    unit: StrOrNone
    period_type: StrOrNone
    isTextBlock_flg: StrOrNone
    abstract_flg: StrOrNone
    period_start: StrOrNone
    period_end: StrOrNone
    instant_date: StrOrNone
    end_date_pv: StrOrNone
    instant_date_pv: StrOrNone
    scenario: StrOrNone


def get_fact_data(fact) -> ArreleFact:
    fact_data = {
        "key": str(fact.qname),
        "data_str": fact.value,
        "decimals": fact.decimals,
        "precision": fact.precision,
        "context_ref": fact.contextID,
        "element_name": str(fact.qname.localName),
        "unit": fact.unitID,  # (str) – unitRef attribute
        "period_type": fact.concept.periodType,  #'instant','duration'
        "isTextBlock_flg": int(fact.concept.isTextBlock),  # 0,1
        "abstract_flg": int(
            fact.concept.abstract == "true"
        ),  # Note: fatc.concept.abstract is str not bool.
    }
    if fact.context.startDatetime:
        fact_data["period_start"] = fact.context.startDatetime.strftime("%Y-%m-%d")
    else:
        fact_data["period_start"] = None
    if fact.context.endDatetime:
        fact_data["period_end"] = fact.context.endDatetime.strftime(
            "%Y-%m-%d"
        )  # 1 day added???
    else:
        fact_data["period_end"] = None
    if fact.context.instantDatetime:
        fact_data["instant_date"] = fact.context.instantDatetime.strftime(
            "%Y-%m-%d"
        )  # 1 day added???
    else:
        fact_data["instant_date"] = None

    fact_data["end_date_pv"] = None
    fact_data["instant_date_pv"] = None
    for item in fact.context.propertyView:
        if item:
            if item[0] == "endDate":
                fact_data["end_date_pv"] = item[1]
            elif item[0] == "instant":
                fact_data["instant_date_pv"] = item[1]
    scenario = []
    for dimension, dim_value in fact.context.scenDimValues.items():
        scenario.append(
            {
                "ja": (
                    dimension.label(preferredLabel=None, lang="ja", linkroleHint=None),
                    dim_value.member.label(
                        preferredLabel=None, lang="ja", linkroleHint=None
                    ),
                ),
                "en": (
                    dimension.label(preferredLabel=None, lang="en", linkroleHint=None),
                    dim_value.member.label(
                        preferredLabel=None, lang="en", linkroleHint=None
                    ),
                ),
                "id": (dimension.id, dim_value.member.id),
            }
        )
    if scenario:
        scenario_json = json.dumps(scenario, ensure_ascii=False, separators=(",", ":"))
    else:
        scenario_json = None

    fact_data["scenario"] = scenario_json
    return fact_data


def get_xbrl_dei_df(xbrl_filename: str, log_dict, temp_dir) -> (xbrl_elm_schima, dict):
    if log_dict["arelle_log_fname"] is None:
        log_dict["arelle_log_fname"] = str(temp_dir / "arelle.log")

    ctrl = Cntlr.Cntlr(logFileName=str(log_dict["arelle_log_fname"]))
    model_xbrl = ctrl.modelManager.load(xbrl_filename)
    localname = "AccountingStandardsDEI"
    qname_prefix = "jpdei_cor"
    ns = model_xbrl.prefixedNamespaces[qname_prefix]

    facts = model_xbrl.factsByQname[qname(ns, name=f"{qname_prefix}:{localname}")]
    fact_list = list(facts)
    if len(fact_list) > 0:
        log_dict[localname] = fact_list[0].value
    else:
        log_dict[localname] = None
    ctrl.close()
    return log_dict


def get_xbrl_df(xbrl_filename: str, log_dict, temp_dir) -> (xbrl_elm_schima, dict):
    """arelle.ModelInstanceObject - Arelle
    https://arelle.readthedocs.io/en/2.18.0/apidocs/arelle/arelle.ModelInstanceObject.html#arelle.ModelInstanceObject.ModelFact
    """
    if log_dict["arelle_log_fname"] is None:
        log_dict["arelle_log_fname"] = str(temp_dir / "arelle.log")

    ctrl = Cntlr.Cntlr(logFileName=str(log_dict["arelle_log_fname"]))
    model_xbrl = ctrl.modelManager.load(xbrl_filename)
    if len(model_xbrl.facts) == 0:
        log_dict["xbrl_load_status"] = "failure"
        ctrl.close()
        return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)), log_dict
    log_dict["xbrl_load_status"] = "success"
    fact_dict_list = []
    for fact in model_xbrl.facts:
        fact_dict_list.append(get_fact_data(fact))
    # log
    ctrl.close()
    return pd.DataFrame(fact_dict_list).drop_duplicates(), log_dict


def get_accounting_standards_dei(
    xbrl_filename: str,
    arelle_log_fname: str,
) -> (xbrl_elm_schima, dict):
    ctrl = Cntlr.Cntlr(logFileName=arelle_log_fname)
    model_xbrl = ctrl.modelManager.load(xbrl_filename)
    localname = "AccountingStandardsDEI"
    qname_prefix = "jpdei_cor"
    ns = model_xbrl.prefixedNamespaces[qname_prefix]

    facts = model_xbrl.factsByQname[qname(ns, name=f"{qname_prefix}:{localname}")]
    # facts=model_xbrl.factsByLocalName[name=f"{qname_prefix}:{localname}"]

    fact_list = list(facts)
    if len(fact_list) > 0:
        accounting_standards = fact_list[0].value
    else:
        accounting_standards = None
    ctrl.close()
    return accounting_standards


class LogParseXBRL(BaseModel):
    is_xbrl_file: bool
    is_xsd_file: bool
    is_def_file: bool
    status: StrOrNone
    error_message: StrOrNone
    already_parse_xbrl: bool
    # AccountingStandardsDEI:StrOrNone


def get_xbrl_rapper(
    docid,
    zip_file: str,
    temp_dir: Path,
    out_path: Path,
    update_flg=False,
    # log_dict=None,
    xbrl_parsed_fname: str = "xbrl_parsed.csv",
):
    # if log_dict is None:
    log_dict = {
        "is_xbrl_file": None,
        "is_xsd_file": None,
        "arelle_log_fname": None,
        "status": None,
        "error_message": None,
        "get_xbrl_status": None,
        "get_xbrl_error_message": None,
        "already_parse_xbrl": None,
        "already_get_accounting_standards": None,
    }

    try:
        already_exist_flg = (out_path / xbrl_parsed_fname).exists()
        get_accounting_standards_status = False
        if (already_exist_flg) & (update_flg == False):
            xbrl_parsed = pd.read_csv(
                out_path / xbrl_parsed_fname,
                dtype=dtype(xbrl_elm_schima),
            )

            filename = str(out_path / "log_dict.json")
            with open(filename, encoding="utf-8") as f:
                log_dict = json.load(f)

            log_dict["already_parse_xbrl"] = True
            get_accounting_standards_status = (
                "AccountingStandardsDEI" in log_dict.keys()
            )
            if get_accounting_standards_status:
                log_dict["already_get_accounting_standards"] = True
                accounting_standards_dei = log_dict["AccountingStandardsDEI"]
            else:
                log_dict["already_get_accounting_standards"] = False
                accounting_standards_dei = None
            # xbrl_parsed['AccountingStandardsDEI'] = log_dict['AccountingStandardsDEI']
            # return xbrl_elm_schima(xbrl_parsed),accounting_standards_dei,log_dict#LogParseXBRL(log_dict)
    except Exception as e:
        print(e)
        already_exist_flg = False
        get_accounting_standards_status = False
    try:
        if already_exist_flg == False:
            log_dict["already_parse_xbrl"] = False
            with ZipFile(str(zip_file)) as zf:
                fn = [
                    item
                    for item in zf.namelist()
                    if (".xbrl" in item) & ("PublicDoc" in item) & ("asr" in item)
                ]
                if len(fn) > 0:
                    zf.extract(fn[0], out_path)
                    log_dict["is_xbrl_file"] = True
                else:
                    log_dict["is_xbrl_file"] = False
                fn = [
                    item
                    for item in zf.namelist()
                    if (".xsd" in item) & ("PublicDoc" in item) & ("asr" in item)
                ]
                if len(fn) > 0:
                    zf.extract(fn[0], out_path)
                    log_dict["is_xsd_file"] = True
                else:
                    log_dict["is_xsd_file"] = False
                fn = [
                    item
                    for item in zf.namelist()
                    if ("def.xml" in item) & ("PublicDoc" in item) & ("asr" in item)
                ]
                if len(fn) > 0:
                    zf.extract(fn[0], out_path)
                    log_dict["is_def_file"] = True
                else:
                    log_dict["is_def_file"] = False
        xbrl_path = out_path / "XBRL" / "PublicDoc"
        assert xbrl_path.exists()
        assert (len(list(xbrl_path.glob("*.xbrl"))) > 0) & (
            len(list(xbrl_path.glob("*.xsd"))) > 0
        )  # &(len(list(xbrl_path.glob("*def.xml")))>0) # xbrl and xsd file exists
        xbrl_filename = str(list(xbrl_path.glob("*.xbrl"))[0])
        if (not already_exist_flg) | (update_flg == True):
            (xbrl_path / "arelle.log").touch()
            xbrl_parsed, log_dict = get_xbrl_df(xbrl_filename, log_dict, temp_dir)
            xbrl_parsed.to_csv(out_path / xbrl_parsed_fname, index=False)
            log_dict["get_xbrl_status"] = "success"
            log_dict["get_xbrl_error_message"] = None
        if not get_accounting_standards_status:
            arelle_log_fname = str(xbrl_path / "arelle.log")
            accounting_standards_dei = get_accounting_standards_dei(
                xbrl_filename,
                arelle_log_fname,
            )
            # xbrl_parsed['AccountingStandardsDEI'] = log_dict['AccountingStandardsDEI']
            log_dict["get_as_status"] = "success"
            log_dict["get_as_error_message"] = None
            # out_filename=str(xbrl_path / "log_dict.json")
            # with open(out_filename, mode="wt", encoding="utf-8") as f:
            #    json.dump(log_dict, f, ensure_ascii=False, indent=2)
        return (
            xbrl_elm_schima(xbrl_parsed),
            accounting_standards_dei,
            log_dict,
        )  # LogParseXBRL(**log_dict)
    except Exception as e:
        log_dict["get_xbrl_status"] = "failure"
        log_dict["get_xbrl_error_message"] = str(e)
        print(e)
        accounting_standards_dei = None
        # out_filename=str(xbrl_path / "log_dict.json")
        # with open(out_filename, mode="wt", encoding="utf-8") as f:
        #    json.dump(log_dict, f, ensure_ascii=False, indent=2)

        return (
            pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)),
            accounting_standards_dei,
            log_dict,
        )


def get_xbrl_rapper_pld(
    docid,
    zip_file: str,
    temp_dir: Path,
    out_path: Path,
    update_flg=False,
    log_dict=None,
):
    if log_dict is None:
        log_dict = {
            "is_xbrl_file": None,
            "is_xsd_file": None,
            "arelle_log_fname": None,
            "status": None,
            "error_message": None,
        }

    try:
        already_exist_flg = (out_path / "xbrl_parsed.csv").exists()
        if (already_exist_flg) & (update_flg == False):
            log_dict["already_parse_xbrl"] = True
            xbrl_parsed = pd.read_csv(
                out_path / "xbrl_parsed.csv", dtype=dtype(xbrl_elm_schima)
            )
            return xbrl_elm_schima(xbrl_parsed), log_dict
    except Exception:
        already_exist_flg = False
    try:
        log_dict["already_parse_xbrl"] = False
        # data_dir_raw=PROJDIR / "data" / "1_raw"
        # zip_file = list(data_dir_raw.glob("data_pool_*/"+docid+".zip"))[0]
        with ZipFile(str(zip_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if (".xbrl" in item) & ("PublicDoc" in item) & ("asr" in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], out_path)
                log_dict["is_xbrl_file"] = True
            else:
                log_dict["is_xbrl_file"] = False
            fn = [
                item
                for item in zf.namelist()
                if (".xsd" in item) & ("PublicDoc" in item) & ("asr" in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], out_path)
                log_dict["is_xsd_file"] = True
            else:
                log_dict["is_xsd_file"] = False
            fn = [
                item
                for item in zf.namelist()
                if ("def.xml" in item) & ("PublicDoc" in item) & ("asr" in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], out_path)
                log_dict["is_def_file"] = True
            else:
                log_dict["is_def_file"] = False
        xbrl_path = out_path / "XBRL" / "PublicDoc"

        if (
            (len(list(xbrl_path.glob("*.xbrl"))) > 0)
            & (len(list(xbrl_path.glob("*.xsd"))) > 0)
            & (len(list(xbrl_path.glob("*def.xml"))) > 0)
        ):  # xbrl and xsd file exists
            xbrl_filename = str(list(xbrl_path.glob("*.xbrl"))[0])
            if (not already_exist_flg) | (update_flg == True):
                (xbrl_path / "arelle.log").touch()
                xbrl_parsed, log_dict = get_xbrl_df(xbrl_filename, log_dict, temp_dir)
                xbrl_parsed.to_csv(out_path / "xbrl_parsed.csv", index=False)

                log_dict = get_xbrl_dei_df(xbrl_filename, log_dict, temp_dir)
                xbrl_parsed["AccountingStandardsDEI"] = log_dict[
                    "AccountingStandardsDEI"
                ]
                log_dict["get_xbrl_status"] = "success"
                log_dict["get_xbrl_error_message"] = None

            else:
                log_dict["get_xbrl_status"] = "success"
                log_dict["get_xbrl_error_message"] = None
            out_filename = str(xbrl_path / "log_dict.json")
            with open(out_filename, mode="w", encoding="utf-8") as f:
                json.dump(log_dict, f, ensure_ascii=False, indent=2)

            return xbrl_parsed, log_dict
        log_dict["get_xbrl_status"] = "failure"
        log_dict["get_xbrl_error_message"] = "No xbrl or xsd file"
        out_filename = str(xbrl_path / "log_dict.json")
        with open(out_filename, mode="w", encoding="utf-8") as f:
            json.dump(log_dict, f, ensure_ascii=False, indent=2)
        return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)), log_dict
    except Exception as e:
        log_dict["get_xbrl_status"] = "failure"
        log_dict["get_xbrl_error_message"] = e
        out_filename = str(xbrl_path / "log_dict.json")
        with open(out_filename, mode="w", encoding="utf-8") as f:
            json.dump(log_dict, f, ensure_ascii=False, indent=2)

        return pd.DataFrame(columns=get_columns_df(xbrl_elm_schima)), log_dict
