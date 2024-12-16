


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





def get_fs_tbl(account_list_common_obj,docid,zip_file_str,temp_path_str,role_keyward_list):
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
        temp_dir=temp_path_str,
        out_path=temp_path_str,
        update_flg=False
        )
    #role_keyward_list = []
    for role_t in role_keyward_list:
        role_list = [role_key for role_key in list(linkbasefile_obj.account_tbl_role_dict.keys()) if role_t in role_key]
    data_list = []
    for role in role_list:
        key_in_the_rol:pd.Series = linkbasefile_obj.account_tbl_role_dict[role].key
        data=pd.merge(
            xbrl_data_df.query("key in @key_in_the_role"),
            linkbasefile_obj.account_tbl_role_dict[role],
            on='key',
            how='left')#.query("not key.str.contains('Abstract')")
        data = data.assign(docid=docid,role=role)
        
        data.assign(
            non_consolidated_flg=data.context_ref.str.contains('NonConsolidated'),
            current_flg=data.context_ref.str.contains('CurrentYear'),
            prior_flg=data.context_ref.str.contains('Prior1Year')
            
        )
        data_list.append(data)
    return pd.concat(data_list)


class fs_tbl_loader():
    def __init__(self,account_list_common_obj,docid,zip_file_str,temp_path_str,role_label_list=['BS','PL','CF','SS','NOTES'],role_list=[]):
        self.linkbasefile_obj = linkbasefile(
            zip_file_str=zip_file_str,
            temp_path_str=temp_path_str
            )
        with timer("read_linkbase_file"):
            self.linkbasefile_obj.read_linkbase_file()
        self.linkbasefile_obj.check()
        self.linkbasefile_obj.make_account_label(account_list_common_obj,role_label_list,role_list)
        self.docid = docid
        with timer("get_xbrl_rapper"):
            self.xbrl_data_df,self.log_dict = get_xbrl_rapper(
                docid=docid,
                zip_file=zip_file_str,
                temp_dir=temp_path_str,
                out_path=temp_path_str,
                update_flg=False)
        self.cnt_dict = {
            "cnt_current_non_consolidated": len(self.xbrl_data_df.query("context_ref.str.contains('CurrentYear') and (not context_ref.str.contains('NonConsolidated'))")),
            "cnt_current_consolidated": len(self.xbrl_data_df.query("context_ref.str.contains('CurrentYear') and context_ref.str.contains('NonConsolidated')")),
            "cnt_prior_non_consolidated": len(self.xbrl_data_df.query("context_ref.str.contains('Prior1Year') and (not context_ref.str.contains('NonConsolidated'))")),
            "cnt_prior_consolidated": len(self.xbrl_data_df.query("context_ref.str.contains('Prior1Year') and context_ref.str.contains('NonConsolidated')")),
            "all": len(self.xbrl_data_df)
        }
    def get_data_from_key(self,key_list:list,term='current',Consolidated=True):
        """keyごとに取得"""
        #data_list = []
        if Consolidated:
            if term == 'current':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and context_ref.str.contains('CurrentYear') and (not context_ref.str.contains('NonConsolidated'))")
            elif term == 'prior':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and context_ref.str.contains('Prior1Year') and (not context_ref.str.contains('NonConsolidated'))")
            elif term == 'all':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and (not context_ref.str.contains('NonConsolidated'))")
        else:
            if term == 'current':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and context_ref.str.contains('CurrentYear') and context_ref.str.contains('NonConsolidated')")
            elif term == 'prior':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and context_ref.str.contains('Prior1Year') and context_ref.str.contains('NonConsolidated')")
            elif term == 'all':
                xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_list and context_ref.str.contains('NonConsolidated')")
                
        data=pd.merge(
            xbrl_data_ext_df,
            self.linkbasefile_obj.account_tbl_role_dict[role],
            on='key',
            how='left')#.query("not key.str.contains('Abstract')")
        data = data.assign(docID=self.docid,role=role)
        #data_list.append(data)
        return data#pd.concat(data_list)

    def get_data(self,doc_name='BS',term='current',Consolidated=True):
        """roleまとめて取得"""
        assert doc_name in ['BS','PL','CF','SS','NOTES','report'],"doc_name should be one of ['BS','PL','CF','SS','NOTES']"
        assert term in ['current','prior','all'],"term should be one of ['current','prior','all']"
        assert isinstance(Consolidated,bool),"Consolidated should be boolean"
    
        fs_dict={
                'BS':["_BalanceSheet","_ConsolidatedBalanceSheet"],
                'PL':["_StatementOfIncome","_ConsolidatedStatementOfIncome"],
                'CF':["_StatementOfCashFlows","_ConsolidatedStatementOfCashFlows"],
                'SS':["_StatementOfChangesInEquity","_ConsolidatedStatementOfChangesInEquity"],
                'NOTES':["_Notes","_ConsolidatedNotes"],
                'report':["_CabinetOfficeOrdinanceOnDisclosure"]}
        #fs_dict={
        #    'BS':"_BalanceSheet",
        #    'PL':"_StatementOfIncome",
        #    'CF':"_StatementOfCashFlows",
        #    'SS':"_StatementOfChangesInEquity",
        #    'NOTES':"_Notes"}
        role_list = []
        for role_t in fs_dict[doc_name]:
            role_list = [role_key for role_key in list(self.linkbasefile_obj.account_tbl_role_dict.keys()) if role_t in role_key]
        data_list = []
        for role in role_list:
            key_in_the_role = self.linkbasefile_obj.account_tbl_role_dict[role].key
            print(len(key_in_the_role))
            # TODO: context_ref == FilingDateInstantの処理
            if Consolidated:
                if term == 'current':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and context_ref.str.contains('CurrentYear') and (not context_ref.str.contains('NonConsolidated'))")
                elif term == 'prior':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and context_ref.str.contains('Prior1Year') and (not context_ref.str.contains('NonConsolidated'))")
                elif term == 'all':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and (not context_ref.str.contains('NonConsolidated'))")
            else:
                if term == 'current':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and context_ref.str.contains('CurrentYear') and context_ref.str.contains('NonConsolidated')")
                elif term == 'prior':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and context_ref.str.contains('Prior1Year') and context_ref.str.contains('NonConsolidated')")
                elif term == 'all':
                    xbrl_data_ext_df = self.xbrl_data_df.query("key in @key_in_the_role and context_ref.str.contains('NonConsolidated')")
            data=pd.merge(
                xbrl_data_ext_df,
                self.linkbasefile_obj.account_tbl_role_dict[role],
                on='key',
                how='left')#.query("not key.str.contains('Abstract')")
            data = data.assign(docID=self.docid,role=role)
            data_list.append(data)
        return pd.concat(data_list)

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
        assert len(p_key_set-all_key_set) == 0
        assert len(c_key_set-all_key_set) == 0
        #print(len(set(self.account_list.label)))
        assert len(set(self.label_tbl_jp.key) - all_key_set) == 0

    def make_account_label(self,account_list_common_obj,role_list,role_label_list=[]):
        account_label_org = self.make_account_label_org()
        account_label_common = self.make_account_label_common(account_list_common_obj)
        account_label = pd.concat([account_label_org,account_label_common],axis=0)
        with timer("make_account_tbl"):
            account_tbl = pd.merge(
                self.account_list[['key','role']],
                account_label[['label_jp']],
                left_on='key',
                right_index=True,
                how='left')
        with timer("make_account_link_tracer"):
            self.account_link_tracer_obj = account_link_tracer(self.parent_child_df)
        
        with timer("make_account_tbl_role_dict"):
            fs_dict={
                'BS':["_BalanceSheet","_ConsolidatedBalanceSheet"],
                'PL':["_StatementOfIncome","_ConsolidatedStatementOfIncome"],
                'CF':["_StatementOfCashFlows","_ConsolidatedStatementOfCashFlows"],
                'SS':["_StatementOfChangesInEquity","_ConsolidatedStatementOfChangesInEquity"],
                'NOTES':["_Notes","_ConsolidatedNotes"],
                'report':["_CabinetOfficeOrdinanceOnDisclosure"]}
            role_list_all = list(set(self.parent_child_df.role))
            role_list_f = []
            if len(role_label_list)>0:
                for role_to_get in role_label_list:
                    for role_t in fs_dict[role_to_get]:
                        role_list_f = role_list_f + [role_key for role_key in role_list_all if role_t in role_key]
            else:
                role_list_f = role_list_all
            
            if len(role_list)>0:
                # role_list が優先される
                role_list_f = []
                for role_t in role_list:
                    role_list_f = role_list_f + [role_key for role_key in role_list_all if role_t in role_key]

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

        #pl_account=account_tbl.query("role.str.contains('StatementOfIncome')").drop_duplicates()
        #bs_account=account_tbl.query("role.str.contains('BalanceSheet')").drop_duplicates()

        #role_text="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_StatementOfIncome"
        #pl_account_tbl=pd.merge(
        #    pl_account,
        #    self.account_link_tracer_obj.get_child_order_recursive_list(
        #        key_list=pl_account.key.to_list(),
        #        role=role_text
        #    )[['order','child_key']],
        #    left_on='key',
        #    right_on='child_key',
        #    how='left')
        #pl_account_tbl.order.fillna(1,inplace=True)
        #self.pl_account_tbl=pl_account_tbl.sort_values('order')
        #role_text="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_StatementOfIncome"

    def make_account_label_org(self):
        df=self.label_tbl_jp.query("role == 'label'").set_index("key").rename(columns={"text":"label_jp"})
        df.join([
            self.label_tbl_jp.query("role == 'verboseLabel'").set_index("key")[['text']].rename(columns={"text":"label_jp_long"}),
            self.label_tbl_eng.query("role == 'label'").set_index("key")[['text']].rename(columns={"text":"label_eng"}),
            self.label_tbl_eng.query("role == 'verboseLabel'").set_index("key")[['text']].rename(columns={"text":"label_eng_long"})
        ],how="left")
        print("org",len(df))
        return df
    
    def make_summary_tbl(self):
        df=pd.DataFrame(index=list(set(self.account_list.key)))
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
        print("common:",len(account_label_common))
        return account_label_common
        