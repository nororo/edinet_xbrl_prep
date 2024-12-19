"""
財務情報をまとめてpandas.DataFrameで出力するモジュール
"""


import xml.etree.ElementTree as ET
import re
from zipfile import ZipFile
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series



from datetime import datetime, timedelta, date
from pydantic import BaseModel, Field
from time import sleep
from typing import Literal
import json
from typing import Annotated
from pydantic.functional_validators import BeforeValidator

from pathlib import Path
import requests
from .xbrl_parser_rapper import *
from .link_base_file_analyzer import *
from .utils import *



class FsDataDf(pa.DataFrameModel):
    #ParentChildLink
    """
    'key': taxonomy like 'jpcrp_cor:NetSales'
    'data_str': data (string) like '1000000'
    'decimals': 3桁の表示
    'precision': ???
    'context_ref': # T:-3, M:-6, B:-9
    'element_name':
    'unit': # JPY
    'period_type':
    'isTextBlock_flg':
    'abstract_flg':
    'period_start': # durationの場合 当期末日, instantの場合 None
    'period_end': # durationの場合 当期末日, instantの場合 当期末日
    'instant_date': # durationの場合 None, instantの場合 当期末日
    'end_date_pv': # durationの場合 前期末日, instantの場合 None
    'instant_date_pv': # durationの場合 None, instantの場合 前期対象日
    'scenario':# シナリオ
    'role': #
    'label_jp':
    'label_jp_long':
    'label_en':
    'label_en_long':    
    'order':
    'child_key':
    'docid':
    """
    key: Series[str] = pa.Field(nullable=True)
    data_str: Series[str] = pa.Field(nullable=True)
    decimals: Series[str] = pa.Field(nullable=True)
    #precision: Series[str] = pa.Field(nullable=True)
    context_ref: Series[str] = pa.Field(nullable=True)
    element_name: Series[str] = pa.Field(nullable=True)
    unit: Series[str] = pa.Field(nullable=True)
    period_type: Series[str] = pa.Field(isin=['instant','duration'],nullable=True) # 'instant','duration'
    isTextBlock_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    abstract_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    period_start: Series[str] = pa.Field(nullable=True)
    period_end: Series[str] = pa.Field(nullable=True)
    instant_date: Series[str] = pa.Field(nullable=True)
    end_date_pv: Series[str] = pa.Field(nullable=True)
    instant_date_pv: Series[str] = pa.Field(nullable=True)
    scenario: Series[str] = pa.Field(nullable=True)
    role: Series[str] = pa.Field(nullable=True)
    label_jp: Series[str] = pa.Field(nullable=True)
    label_jp_long: Series[str] = pa.Field(nullable=True)
    label_en: Series[str] = pa.Field(nullable=True)
    label_en_long: Series[str] = pa.Field(nullable=True)
    order: Series[float] = pa.Field(nullable=True)
    #child_key: Series[str] = pa.Field(nullable=True)
    docid: Series[str] = pa.Field(nullable=True)
    non_consolidated_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    current_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    prior_flg: Series[int] = pa.Field(isin=[0,1],nullable=True) # 0,1
    AccountingStandardsDEI: Series[str] = pa.Field(nullable=True)



def get_fs_tbl(account_list_common_obj,docid:str,zip_file_str:str,temp_path_str:str,role_keyward_list:list)->FsDataDf:
    linkbasefile_obj = linkbasefile(
        zip_file_str=zip_file_str,
        temp_path_str=temp_path_str
        )
    linkbasefile_obj.read_linkbase_file()
    linkbasefile_obj.check()
    linkbasefile_obj.make_account_label(account_list_common_obj,role_keyward_list)
    xbrl_data_df,log_dict = get_xbrl_rapper(
        docid=docid,
        zip_file=zip_file_str,
        temp_dir=Path(temp_path_str),
        out_path=Path(temp_path_str),
        update_flg=False
        )
    data_list = []
    for role in list(linkbasefile_obj.account_tbl_role_dict.keys()):
        key_in_the_role:pd.Series = linkbasefile_obj.account_tbl_role_dict[role].key
        data=pd.merge(
            xbrl_data_df.query("key in @key_in_the_role"),
            linkbasefile_obj.account_tbl_role_dict[role],
            on='key',
            how='left')
        data = data.assign(docid=docid,role=role)
        
        data = data.assign(
            non_consolidated_flg=data.context_ref.str.contains('NonConsolidated').astype(int),
            current_flg=data.context_ref.str.contains('CurrentYear').astype(int),
            prior_flg=data.context_ref.str.contains('Prior1Year').astype(int)
        )
        data['label_jp'] = data.label_jp.fillna('-')
        data['label_jp_long'] = data.label_jp_long.fillna('-')
        data['label_en'] = data.label_en.fillna('-')
        data['label_en_long'] = data.label_en_long.fillna('-')

        data = data.query("(not (non_consolidated_flg==1 and role.str.contains('_Consolidated'))) and (not (non_consolidated_flg==0 and (not role.str.contains('_Consolidated') and not (role.str.contains('_CabinetOfficeOrdinanceOnDisclosure')))))")
        data_list.append(data)
    return FsDataDf(pd.concat(data_list)[get_columns_df(FsDataDf)])



class linkbasefile():
    def __init__(self,zip_file_str:str,temp_path_str:str):
        self.zip_file_str = zip_file_str
        self.temp_path_str = temp_path_str
        self.log_dict = {}
    def read_linkbase_file(self):
        self.get_presentation_account_list_obj = get_presentation_account_list(
            zip_file_str=self.zip_file_str,
            temp_path_str=self.temp_path_str,
            doc_type='public'
        )
        self.parent_child_df = self.get_presentation_account_list_obj.export_parent_child_link_df()
        self.account_list = self.get_presentation_account_list_obj.export_account_list_df()
        self.log_dict = {**self.log_dict,**self.get_presentation_account_list_obj.export_log().model_dump()}
        
        self.get_calc_edge_list_obj = get_calc_edge_list(
            zip_file_str=self.zip_file_str,
            temp_path_str=self.temp_path_str
            )
        self.calc_edge_df = self.get_calc_edge_list_obj.export_parent_child_link_df()
        self.get_label_obj_jp = get_label(
            lang="Japanese",
            zip_file_str=self.zip_file_str,
            temp_path_str=self.temp_path_str
            )
        self.label_tbl_jp = self.get_label_obj_jp.export_label_tbl(label_to_taxonomi_dict=self.get_presentation_account_list_obj.export_label_to_taxonomi_dict())
        #self.log_dict={**self.log_dict,**self.get_label_obj_jp.export_log().model_dump()}
        self.get_label_obj_eng = get_label(
            lang="English",
            zip_file_str=self.zip_file_str,
            temp_path_str=self.temp_path_str
            )
        self.label_tbl_eng = self.get_label_obj_eng.export_label_tbl(label_to_taxonomi_dict=self.get_presentation_account_list_obj.export_label_to_taxonomi_dict())

    def check(self):
        p_key_set = set(self.parent_child_df.parent_key)
        #print(len(p_key_set))
        c_key_set = set(self.parent_child_df.child_key)
        #print(len(c_key_set))
        all_key_set = set(self.account_list.key)
        if len(p_key_set-all_key_set) != 0:
            print("parent key in arc-link that is not included in locator: \n{}".format(str(p_key_set-all_key_set)))
        if len(c_key_set-all_key_set) != 0:
            print("child key in arc-link that is not included in locator: \n{}".format(str(p_key_set-all_key_set)))
        #print(len(set(self.account_list.label)))
        if len(set(self.label_tbl_jp.key) - all_key_set) != 0:
            print("key in label that is not included in locator: \n{}".format(str(set(self.label_tbl_jp.key) - all_key_set)))

    def make_account_label(self,account_list_common_obj,role_list,role_label_list=[]):
        account_label_org = self.make_account_label_org()
        account_label_common = self.make_account_label_common(account_list_common_obj)
        account_label = pd.concat([account_label_org,account_label_common],axis=0)
        account_tbl = pd.merge(
            self.account_list[['key','role']],
            account_label[['label_jp','label_jp_long','label_en','label_en_long']],
            left_on='key',
            right_index=True,
            how='left')
        self.account_link_tracer_obj = account_link_tracer(self.parent_child_df)
        
        role_list_all = list(set(self.parent_child_df.role))
            
        if len(role_list)>0:
            # role_list が優先される
            role_list_f = []
            for role_t in role_list:
                role_list_f = role_list_f + [role_key for role_key in role_list_all if role_t in role_key]
        else:
            role_list_f = role_list_all
        account_tbl_role_dict = {}
        for role_text in role_list_f:
            role_suffix = role_text.split('/')[-1]
            account_tbl_of_the_role = account_tbl.query("role.str.contains(@role_suffix)").drop_duplicates()
            account_tbl_of_the_role = pd.merge(
                account_tbl_of_the_role,
                self.account_link_tracer_obj.get_child_order_recursive_list(
                    key_list=account_tbl_of_the_role.key.to_list(),
                    role=role_text
                )[['order','child_key']],
                left_on='key',
                right_on='child_key',
                how='left')
            account_tbl_of_the_role.order.fillna(1,inplace=True)
            account_tbl_of_the_role.sort_values('order')
            account_tbl_role_dict.update({role_suffix:account_tbl_of_the_role})
        self.account_tbl_role_dict = account_tbl_role_dict

    def make_account_label_org(self):
        df=self.label_tbl_jp.query("role == 'label'").set_index("key").rename(columns={"text":"label_jp"})
        df.join([
            self.label_tbl_jp.query("role == 'verboseLabel'").set_index("key")[['text']].rename(columns={"text":"label_jp_long"}),
            self.label_tbl_eng.query("role == 'label'").set_index("key")[['text']].rename(columns={"text":"label_en"}),
            self.label_tbl_eng.query("role == 'verboseLabel'").set_index("key")[['text']].rename(columns={"text":"label_en_long"})
        ],how="left")
        #print("org",len(df))
        return df
    
    def make_summary_tbl(self):
        df = pd.DataFrame(index=list(set(self.account_list.key)))
        df = df.assign(
            is_parent=df.index.isin(self.parent_child_df.parent_key),
            is_child=df.index.isin(self.parent_child_df.child_key),
            is_calc_parent=df.index.isin(self.calc_edge_df.parent_key),
            is_calc_child=df.index.isin(self.calc_edge_df.child_key)
            )

    def detect_account_list_year(self):
        head_list=list(set(self.get_presentation_account_list_obj.export_account_list_df().schima_taxonomi_head))
        head_jpcrp=[head for head in head_list if "http://disclosure.edinet-fsa.go.jp/taxonomy/jpcrp" in head][0]
        if "2023-12-01" in head_jpcrp:
            self.account_list_year="2024"
        elif "2022-11-01" in head_jpcrp:
            self.account_list_year="2023"
        elif "2021-11-01" in head_jpcrp:
            self.account_list_year="2022"
        elif "2020-11-01" in head_jpcrp:
            self.account_list_year="2021"
        elif "2019-11-01" in head_jpcrp:
            self.account_list_year="2020"
        else:
            self.account_list_year="-"
        return self.account_list_year
    
    def make_account_label_common(self,account_list_common_obj):
        self.detect_account_list_year()
        #self.account_list_common_obj = account_list_common_obj
        #label_to_taxonomi_dict=self.get_presentation_account_list_obj.export_label_to_taxonomi_dict()
        
        account_label_common = account_list_common_obj.get_assign_common_label()
        #print("common:",len(account_label_common))
        return account_label_common
        