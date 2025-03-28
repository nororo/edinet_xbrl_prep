"""
リンクベースファイル解析用モジュール
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
from .utils import *
# %%
StrOrNone = Annotated[str, BeforeValidator(lambda x: x or "")]
FloatOrNone = Annotated[float, BeforeValidator(lambda x: x or 0.0)]

# %% #################################################################
#
#            schima
#
######################################################################

#def get_columns_df(schima:pa.DataFrameModel)->list:
#    return list(schima.to_schema().columns.keys())


class GetCalLog(BaseModel):
    is_cal_file_flg: int = Field(isin=[0,1], title="is_cal_file_flg", description="0: not exist, 1: exist")
    get_cal_status: Literal['success','failure'] = Field('succsess', title="result", description="success or failure")
    get_cal_error_message: StrOrNone = Field(default="", title="message", description="message")

class GetPresentationLog(BaseModel):
    is_pre_file_flg: int = Field(isin=[0,1], title="is_pre_file_flg", description="0: not exist, 1: exist")
    #status: Literal['success','failure'] = Field('succsess', title="result", description="success or failure")
    #error_message: StrOrNone = Field(default="", title="message", description="message")
    get_pre_status: Literal['success','failure'] = Field('succsess', title="result", description="success or failure")
    get_pre_error_message: StrOrNone = Field(default="", title="message", description="message")

class ParentChildLink(pa.DataFrameModel):
    #ParentChildLink
    """
    KEY:
        parent: jppfs_cor_CameCaseAccountName
        child: jppfs_cor_CameCaseAccountName
        
        parent_taxonomi_tag: jppfs_cor_accountname
        child_taxonomi_tag: jppfs_cor_accountname
        
            taxonomi_tag <- locators_df.schima_taxonomi.str.lower()
            schima_taxonomi <- attr_sr[attr_sr.index.str.contains('href')].values[0].split('#')[1]
    """
    parent_key: Series[str]# = pa.Column(str, checks=pa.Check.str_contains(':'), regex=True, nullable=True)
    child_key: Series[str]# = pa.Column(str, checks=pa.Check.str_contains(':'), regex=True, nullable=True)
    role: Series[str]
    child_order: Series[str]


class CalParentChildLink(pa.DataFrameModel):
    """
    KEY:
        parent_taxonomi_tag: jppfs_cor_accountname
        child_taxonomi_tag: jppfs_cor_accountname
        
            taxonomi_tag <- locators_df.schima_taxonomi.str.lower()
            schima_taxonomi <- attr_sr[attr_sr.index.str.contains('href')].values[0].split('#')[1]
    """

    parent_key: Series[str]
    child_key: Series[str]
    weight: Series[float]
    role: Series[str]


class OriginalAccountList(pa.DataFrameModel):
    #OriginalAccountList
    """
        label: 
        key: jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF
        role: 
        (schima_taxonomi: schima_taxonomi like)
            jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
            sepalated it by '#' and get later part that is jpcrp030000-asr-001_E37207-000_2023-06-30_01_2023-09-29.xsd#jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
            (from xlink:href in pre.xml file) 
    """
    #schima_taxonomi: Series[str]
    label: Series[str]
    key: Series[str]
    role: Series[str]
    schima_taxonomi_head: Series[str]

class AccountLabel(pa.DataFrameModel):
    #OriginalAccountList
    """
        label: 
        key: jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF
        role: 
        (schima_taxonomi: schima_taxonomi like)
            jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
            sepalated it by '#' and get later part that is jpcrp030000-asr-001_E37207-000_2023-06-30_01_2023-09-29.xsd#jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
            (from xlink:href in pre.xml file) 
    """
    #schima_taxonomi: Series[str]
    label: Series[str]
    key: Series[str]
    text: Series[str]
    role: Series[str]
    lang: Series[str]

#def format_taxonomi(taxonomi_str:str)->str:
#    """
#    Convert
#        From:
#        jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
#        To:
#        jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF
#    """
#    return "_".join(taxonomi_str.split('_')[:-1])+":"+taxonomi_str.split('_')[-1]



class PreLocator(BaseModel):
    role: StrOrNone
    schima_taxonomi: StrOrNone
    label: StrOrNone
    schima_taxonomi_head: StrOrNone

class Locator(BaseModel):
    role: StrOrNone
    schima_taxonomi: StrOrNone
    label: StrOrNone

class Arc(BaseModel):
    role: StrOrNone
    parent: StrOrNone
    child: StrOrNone
    child_order: StrOrNone
    
class CalArc(BaseModel):
    role: StrOrNone
    parent: StrOrNone
    child: StrOrNone
    child_order: StrOrNone
    weight: FloatOrNone

class LabArc(BaseModel):
    label_pre: StrOrNone
    label_lab: StrOrNone

class Resource(BaseModel):
    label_lab: StrOrNone
    lang: StrOrNone
    role: StrOrNone
    text: StrOrNone


# %% #################################################################
#
#            account_link_tracer
#
######################################################################

#def remove_empty_lists(lst):
#    return [x for x in lst if x]


#def flatten_list(lst):
#    flat_list = []
#    for item in lst:
#        if isinstance(item, list):
#            flat_list.extend(flatten_list(item))
#        else:
#            flat_list.append(item)
#    return flat_list

class account_link_tracer():
    """
        TODO: get_child_itemsのorder 0.01倍ではもともと実数が入っている場合に対応できていない
    """
    def __init__(self, tbl:ParentChildLink):
        self.parent_child_tbl = tbl.copy()
        self.all_key_list = list(set(self.parent_child_tbl.parent_key)|set(self.parent_child_tbl.child_key))
        self.key_count = len(self.all_key_list)
        self.all_roles = list(set(self.parent_child_tbl.role))
        self.top_role = 'http://disclosure.edinet-fsa.go.jp/role/jpcrp/rol_CabinetOfficeOrdinanceOnDisclosureOfCorporateInformationEtcFormNo3AnnualSecuritiesReport'
        self.other_roles = [role for role in self.all_roles if role!=self.top_role]

    def get_all_roles(self,include_top=True)->list:
        if include_top:
            return self.all_roles
        else:
            return self.other_roles

    def get_child_keys(self,parent_key:str,role:str)->list:
        if len(self.parent_child_tbl.query("parent_key==@parent_key and role == @role"))>0:
            return self.parent_child_tbl.query("parent_key==@parent_key and role == @role").child_key.to_list()
        else:
            return []
    def get_child_items(self,parent_key:str,role:str,now=0,rec:int=0)->list:
        tmp_tbl=self.parent_child_tbl.copy()
        tmp_tbl.child_order=tmp_tbl.child_order.astype(float)*((0.01)**rec)+now
        return tmp_tbl.query("parent_key==@parent_key and role == @role")[['child_key','role','child_order']].to_dict('records')

    def get_parent_keys(self,child_key:str,role:str)->list:
        if len(self.parent_child_tbl.query("child_key==@child_key and role == @role"))>0:
            return self.parent_child_tbl.query("child_key==@child_key and role == @role").parent_key.to_list()
        else:
            return []
    def get_parent_items(self,child_key:str,role:str)->list:
        if len(self.parent_child_tbl.query("child_key==@child_key and role == @role"))>0:
            return self.parent_child_tbl.query("child_key==@child_key and role == @role")[['parent_key','role']].to_dict('records')
        else:
            return []
    
    def get_role(self,key:str)->list:
        return list(set(self.parent_child_tbl.query("parent_key==@key").role.to_list()+self.parent_child_tbl.query("child_key==@key").role.to_list()))

    def get_child_keys_recursive(self,parent_key:str,role:str)->list:
        child_key_list = self.get_child_keys(parent_key,role)
        if len(child_key_list)>0:
            return flatten_list(remove_empty_lists(child_key_list + [self.get_child_keys_recursive(child_key,role) for child_key in child_key_list]))
        else:
            return []
    def get_child_items_recursive(self,parent_key:str,role:str,now=0,rec=0)->list:
        #child_key_list = self.get_child_keys(parent_key,role)
        child_item_list = self.get_child_items(parent_key,role,now=now,rec=rec)
        if len(child_item_list)>0:
            return flatten_list(remove_empty_lists(child_item_list + [self.get_child_items_recursive(child_item['child_key'],role,now=child_item['child_order'],rec=rec+1) for child_item in child_item_list]))
        else:
            return []
    
    def get_parent_keys_trace(self,child_key:str,role:str)->list:
        """
        Given role, parent should be unique.
        """
        parent_key_list = self.get_parent_keys(child_key,role)
        if len(parent_key_list)>0:
            return flatten_list(remove_empty_lists([self.get_parent_keys_trace(parent_key,role) for parent_key in parent_key_list]))
        else:
            return [child_key]
    
    def search_keys(self,keyword_in_taxonomy:str)->list:
        return [key for key in self.all_key_list if keyword_in_taxonomy in key]


    def get_child_order_recursive_list(self,key_list:list,role:str)->pd.DataFrame:
        # 1. search common parent keys
        all_keys=[]
        child_keys=key_list
        for key in key_list:
            all_keys=all_keys+self.get_parent_keys_trace(key,role)
            child_keys=child_keys+self.get_child_keys(key,role)

        key_list2=list(set(all_keys))

        # 2. get all child items
        all_keys=[]
        cnt=1
        for key in key_list2:
            all_keys=all_keys+self.get_child_items_recursive(
                key,role=role,now=cnt,rec=1)
            cnt=cnt+1
        all_keys_df=pd.DataFrame(all_keys).query("child_key in @child_keys").rename(columns={'child_order':'order'})
        return all_keys_df.sort_values('order')

class get_presentation_account_list():
    """
    locator:
        (role:)
        href:
        label:
    arc:
        (role:)
        from:
        to:
        order:
        role is given to edge
    """
    def __init__(self,zip_file_str:str,temp_path_str:str,doc_type:str='public'):#->(parent_child_link_schima,,dict,dict):
        
        self.log_dict = {
            #'docID':docid,
            'is_pre_file_flg':None,
            'get_pre_status':None,
            'get_pre_error_message':None
            }
        self.temp_path=Path(temp_path_str)
        self.temp_path.mkdir(parents=True,exist_ok=True)
        if doc_type == 'audit':
            self.doc_type_str = 'aai'
            self.xml_def_path = self.temp_path / "XBRL" / "AuditDoc"
        elif doc_type == 'public':
            self.doc_type_str = 'asr'
            self.xml_def_path = self.temp_path / "XBRL" / "PublicDoc"
        else:
            raise Exception("doc_type must be 'audit' or 'public'")
        
        self.extruct_pre_file_from_xbrlzip(zip_file_str)
        if self.log_dict['get_pre_status']!='failure':
            self.parse_pre_file()
    
    def extruct_pre_file_from_xbrlzip(self,zip_file_str:str):
        try:
            with ZipFile(str(zip_file_str)) as zf:
                fn=[item for item in zf.namelist() if ("pre.xml" in item) & (self.doc_type_str in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            if len(list(self.xml_def_path.glob("*pre.xml")))==0:
                self.log_dict['is_pre_file_flg'] = 0
                raise Exception("No pre.xml file")
            else:
                self.log_dict['is_pre_file_flg'] = 1
            self.log_dict['get_pre_status'] = 'success'
            
        except Exception as e:
            print(e)
            self.log_dict['is_pre_file_flg'] = 0
            self.log_dict['get_pre_status'] = 'failure'
            self.log_dict['get_pre_error_message'] = str(e)
    
    def parse_pre_file(self):
        """
        TODO: preferedLabelの取得を追加
        """
        tree = ET.parse(str(list(self.xml_def_path.glob("*pre.xml"))[0]))
        root = tree.getroot()
        locators = []
        arcs = []
        for child in root:
            attr_sr_p = pd.Series(child.attrib)
            role = attr_sr_p[attr_sr_p.index.str.contains('role')].item()
            for child_of_child in child:
                locator = {'role':role,'schima_taxonomi':None,'label':None,'schima_taxonomi_head':None}
                arc = {'parent':None,'child':None,'child_order':None,'role':role}

                attr_sr = pd.Series(child_of_child.attrib)
                attr_type = attr_sr[attr_sr.index.str.contains('type')].item()
                if attr_type=='locator':
                    locator['schima_taxonomi_head'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[0]
                    locator['schima_taxonomi'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                    locator['label'] = attr_sr[attr_sr.index.str.contains('label')].item()
                    locators.append(PreLocator(**locator))
                elif attr_type=='arc':
                    arc['parent'] = attr_sr[attr_sr.index.str.contains('from')].item()
                    arc['child'] = attr_sr[attr_sr.index.str.contains('to')].item()
                    arc['child_order'] = attr_sr[attr_sr.index.str.contains('order')].item()
                    arcs.append(Arc(**arc))
                
        self.locators = locators
        self.arcs = arcs

    def _make_label_to_taxonomi_dict(self):
        
        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
            )
        self.label_to_taxonomi_dict = locators_df.set_index('label')['key'].to_dict()

    def export_account_list_df(self)->OriginalAccountList:
        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
                                    )
        pre_detail_list = OriginalAccountList(locators_df[get_columns_df(OriginalAccountList)])
        return pre_detail_list
    def export_parent_child_link_df(self)->ParentChildLink:
        self._make_label_to_taxonomi_dict()
        arcs_df = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(subset=['child'])
        arcs_df = arcs_df.assign(
            parent_key = arcs_df.parent.replace(self.label_to_taxonomi_dict),
            child_key = arcs_df.child.replace(self.label_to_taxonomi_dict))

        arcs_df = ParentChildLink(arcs_df[get_columns_df(ParentChildLink)])
        return arcs_df
    
    def export_log(self)->GetPresentationLog:
        return GetPresentationLog(**self.log_dict)
    
    def export_label_to_taxonomi_dict(self):
        self._make_label_to_taxonomi_dict()
        return self.label_to_taxonomi_dict



class get_calc_edge_list():
    def __init__(self,zip_file_str,temp_path_str):
        self.log_dict = {
            #'docID':docid,
            #'org_taxonomi_cnt':None,
            #'org_taxonomi_list':[],
            'is_cal_file_flg':0,
            'get_cal_status':None,
            'get_cal_error_message':None
                }
        self.temp_path = Path(temp_path_str)
        self.doc_type_str = 'asr'
        self.xml_def_path = self.temp_path / "XBRL" / "PublicDoc"

        self.extruct_cal_file_from_xbrlzip(zip_file_str)
        if self.log_dict['get_cal_status']!='failure':
            self.parse_cal_file()

    def extruct_cal_file_from_xbrlzip(self,zip_file_str):
        try:
            with ZipFile(str(zip_file_str)) as zf:
                fn=[item for item in zf.namelist() if ("cal.xml" in item)&(self.doc_type_str in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)

        except Exception as e:
            self.log_dict['is_cal_file_flg'] = 0
            self.log_dict['get_cal_status'] = 'failure'
            self.log_dict['get_cal_error_message'] = str(e)
        
    def parse_cal_file(self):
        tree = ET.parse(str(list(self.xml_def_path.glob("*cal.xml"))[0]))
        root = tree.getroot()

        locators=[]
        arcs=[]
        for child in root:
            attr_sr_p=pd.Series(child.attrib)
            role=attr_sr_p[attr_sr_p.index.str.contains('role')].item()
            for child_of_child in child:
                locator={'schima_taxonomi':None,'label':None,'role':role}
                arc={'parent':None,'child':None,'child_order':None,'weight':None,'role':role}
                attr_sr=pd.Series(child_of_child.attrib)
                attr_type=attr_sr[attr_sr.index.str.contains('type')].item() 
                if attr_type=='locator':
                    locator['schima_taxonomi']=attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                    locator['label']=attr_sr[attr_sr.index.str.contains('label')].item()
                    locators.append(Locator(**locator))

                elif attr_type=='arc':
                    arc['parent']=attr_sr[attr_sr.index.str.contains('from')].item()
                    arc['child']=attr_sr[attr_sr.index.str.contains('to')].item()
                    arc['child_order']=attr_sr[attr_sr.index.str.contains('order')].item()
                    arc['weight']=attr_sr[attr_sr.index.str.contains('weight')].item()
                    arcs.append(CalArc(**arc))
                
        self.locators = locators
        self.arcs = arcs

    def _make_label_to_taxonomi_dict(self):

        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
            )
        self.label_to_taxonomi_dict = locators_df.set_index('label')['key'].to_dict()

    def export_account_list_df(self)->OriginalAccountList:
        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
            )
        cal_detail_list = OriginalAccountList(locators_df[get_columns_df(OriginalAccountList)])
        return cal_detail_list
    
    def export_parent_child_link_df(self)->CalParentChildLink:
        self._make_label_to_taxonomi_dict()
        arcs_df = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(subset=['child'])
        arcs_df = arcs_df.assign(
            parent_key=arcs_df.parent.replace(self.label_to_taxonomi_dict),
            child_key=arcs_df.child.replace(self.label_to_taxonomi_dict),
            weight=arcs_df.weight.astype(float))
        arcs_df = CalParentChildLink(
            arcs_df.drop_duplicates(subset=['parent_key','child_key'])[get_columns_df(CalParentChildLink)]
            )
        return arcs_df
    def export_log(self)->GetCalLog:
        return GetCalLog(**self.log_dict)



class get_label():
    def __init__(self,zip_file_str:str,temp_path_str:str,lang:str='English')->pd.DataFrame:
        self.log_dict = {
            'is_lab_file_flg':1,
            'get_lab_status':'success',
            'get_lab_error_message':None
            }
        self.temp_path=Path(temp_path_str)
        self.lang = lang
        if lang == 'Japanese':
            self.f_keyword = 'lab.xml'
        else:
            self.f_keyword = 'lab-en.xml'
        self.xml_def_path = self.temp_path / "XBRL" / "PublicDoc"
        self.extruct_lab_file_from_xbrlzip(zip_file_str)
        if self.log_dict['get_lab_status']!='failure':
            self.parse_lab_file()

    def extruct_lab_file_from_xbrlzip(self,zip_file_str:Path):

        try:
            
            with ZipFile(str(zip_file_str)) as zf:
                    fn=[item for item in zf.namelist() if self.f_keyword in item]
                    if len(fn)>0:
                        zf.extract(fn[0], self.temp_path)
        except Exception as e:
            print(e)
            self.log_dict['is_lab_file_flg'] = 0
            self.log_dict['get_lab_status'] = 'failure'
            self.log_dict['get_lab_error_message'] = str(e)

    def parse_lab_file(self):
        tree = ET.parse(str(list(self.xml_def_path.glob("*"+self.f_keyword))[0])) # TODO check iregular file name
        root = tree.getroot()

        resources=[]
        arcs=[]
        for child in root:
            for child_of_child in child:
                resource={'label_lab':None,'lang':None,'role':None,'text':None}
                arc={'label_pre':None,'label_lab':None}
                attr_sr=pd.Series(child_of_child.attrib)
                attr_type=attr_sr[attr_sr.index.str.contains('type')].values[0] 
                if attr_type=='resource':
                    resource['label_lab']=attr_sr[attr_sr.index.str.contains('label')].values[0]
                    resource['lang']=attr_sr[attr_sr.index.str.contains('lang')].values[0]
                    resource['role']=attr_sr[attr_sr.index.str.contains('role')].values[0].split('/')[-1]
                    resource['text']=child_of_child.text
                    resources.append(Resource(**resource))
                elif attr_type=='arc':
                    arc['label_pre']=attr_sr[attr_sr.index.str.contains('from')].values[0]
                    arc['label_lab']=attr_sr[attr_sr.index.str.contains('to')].values[0]
                    arcs.append(LabArc(**arc))
                
        self.resources=resources
        self.arcs=arcs

    def _make_label_to_taxonomi_dict(self):
        self.label_to_prelabel_dict = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(subset='label_lab').set_index('label_lab')['label_pre'].to_dict()
        
    def export_label_tbl(self,label_to_taxonomi_dict:dict)->pd.DataFrame:
        self._make_label_to_taxonomi_dict()
        label_tbl = pd.DataFrame([resource.model_dump() for resource in self.resources]).dropna(subset='label_lab')
        label_tbl = AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace('label_',''),
                key=label_tbl.label_lab.replace(self.label_to_prelabel_dict).replace(label_to_taxonomi_dict)
                )[get_columns_df(AccountLabel)]
            )
        return label_tbl
                    
#        return pd.DataFrame(columns=['label_lab','label_pre','lang','role','text'])



# %% common


class get_label_common():
    def __init__(self,file_str:str,lang:str='English')->pd.DataFrame:
        self.log_dict = {
            'is_lab_file_flg':1,
            'get_lab_status':'success',
            'get_lab_error_message':None
            }
        self.file_path=Path(file_str)
        self.lang = lang
        if lang == 'Japanese':
            self.f_keyword = 'lab.xml'
        else:
            self.f_keyword = 'lab-en.xml'
        #self.xml_def_path = self.temp_path / "XBRL" / "PublicDoc"
        #self.extruct_lab_file_from_xbrlzip(zip_file_str)
        if self.log_dict['get_lab_status']!='failure':
            self.parse_lab_file()
            self._make_label_to_taxonomi_dict()
    #def extruct_lab_file_from_xbrlzip(self,zip_file_str:Path):
    #    try:
    #        
    #        with ZipFile(str(zip_file_str)) as zf:
    #                fn=[item for item in zf.namelist() if self.f_keyword in item]
    #                if len(fn)>0:
    #                    zf.extract(fn[0], self.temp_path)
    #    except Exception as e:
    #        print(e)
    #        self.log_dict['is_lab_file_flg'] = 0
    #        self.log_dict['get_lab_status'] = 'failure'
    #        self.log_dict['get_lab_error_message'] = str(e)

    def parse_lab_file(self):
        tree = ET.parse(self.file_path) # TODO check iregular file name
        root = tree.getroot()

        resources=[]
        arcs=[]
        for child in root:
            for child_of_child in child:
                resource={'label_lab':None,'lang':None,'role':None,'text':None}
                arc={'label_pre':None,'label_lab':None}
                attr_sr=pd.Series(child_of_child.attrib)
                attr_type=attr_sr[attr_sr.index.str.contains('type')].values[0] 
                if attr_type=='resource':
                    resource['label_lab']=attr_sr[attr_sr.index.str.contains('label')].values[0]
                    resource['lang']=attr_sr[attr_sr.index.str.contains('lang')].values[0]
                    resource['role']=attr_sr[attr_sr.index.str.contains('role')].values[0].split('/')[-1]
                    resource['text']=child_of_child.text
                    resources.append(Resource(**resource))
                elif attr_type=='arc':
                    arc['label_pre']=attr_sr[attr_sr.index.str.contains('from')].values[0]
                    arc['label_lab']=attr_sr[attr_sr.index.str.contains('to')].values[0]
                    arcs.append(LabArc(**arc))
                
        self.resources=resources
        self.arcs=arcs

    def _make_label_to_taxonomi_dict(self):
        self.label_to_prelabel_dict = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(subset='label_lab').set_index('label_lab')['label_pre'].to_dict()
        label_tbl = pd.DataFrame([resource.model_dump() for resource in self.resources]).dropna(subset='label_lab')
        self.label_tbl = label_tbl.assign(
            key_all=label_tbl.label_lab.replace(self.label_to_prelabel_dict)#.replace(label_to_taxonomi_dict)
            )
        
    def export_label_tbl2(self,label_to_taxonomi_dict:dict)->pd.DataFrame:
        self._make_label_to_taxonomi_dict()
        label_tbl = pd.DataFrame([resource.model_dump() for resource in self.resources]).dropna(subset='label_lab')
        label_tbl = label_tbl.assign(
                key_all=label_tbl.label_lab.replace(self.label_to_prelabel_dict)#.replace(label_to_taxonomi_dict)
                ).query("key_all in @label_to_taxonomi_dict.keys()")
        label_tbl=AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace('label_',''),
                key=label_tbl.key_all.replace(label_to_taxonomi_dict)
                )#[get_columns_df(AccountLabel)]
            )
        return label_tbl
    
    def export_label_tbl(self,label_to_taxonomi_dict:dict)->pd.DataFrame:
        """
        TODO: change label to taxonomi
        """
        label_tbl=self.label_tbl.query("key_all in @label_to_taxonomi_dict.keys()")
        #print("label: ",len(label_tbl))
        label_tbl=AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace('label_',''),
                key=label_tbl.key_all.replace(label_to_taxonomi_dict)
                )[get_columns_df(AccountLabel)]
            )
        return label_tbl


class account_list_common():
    """
    共通タクソノミの取得。主にリンクベースファイルでimportされているlabel情報を取得する。
    """
    def __init__(self,data_path:str,account_list_year:str):

        linkfiles_dict={
            'pre.xml':"jpcrp030000-asr",
            'lab.xml':"jpcrp",
            'lab-en.xml':"jpcrp"}
        schima_word_list=['jppfs','jpcrp']
        self.taxonomy_file = data_path / "taxonomy_{}.zip".format(account_list_year)
        self.account_list_year = account_list_year
        self.temp_path = data_path / "tmp/taxonomy"
        self.temp_path.mkdir(parents=True, exist_ok=True)
        self.taxonomy_path = data_path / ("taxonomy_" + str(account_list_year))
        self.taxonomy_path.mkdir(parents=True, exist_ok=True)
        self._download_taxonomy()
        self.path_jpcrp_lab = self._download_jpcrp_lab()
        self.path_jpcrp_lab_en = self._download_jpcrp_lab_en()
        self.path_jppfs_lab = self._download_jppfs_lab()
        self.path_jppfs_lab_en = self._download_jppfs_lab_en()
        self.path_jpcrp_pre = self._download_jpcrp_pre()
        self.path_jppfs_pre_list = self._download_jppfs_pre()
        self._build()

    def _download_taxonomy(self):
        download_link_dict = {
            '2024':"https://www.fsa.go.jp/search/20231211/1c_Taxonomy.zip",
            "2023":"https://www.fsa.go.jp/search/20221108/1c_Taxonomy.zip",
            "2022":"https://www.fsa.go.jp/search/20211109/1c_Taxonomy.zip",
            "2021":"https://www.fsa.go.jp/search/20201110/1c_Taxonomy.zip",
            "2020":"https://www.fsa.go.jp/search/20191101/1c_Taxonomy.zip",
            "2019":"https://www.fsa.go.jp/search/20190228/1c_Taxonomy.zip",
            "2018":"https://www.fsa.go.jp/search/20180228/1c_Taxonomy.zip",
            "2017":"https://www.fsa.go.jp/search/20170228/1c.zip",
            "2016":"https://www.fsa.go.jp/search/20160314/1c.zip",
            "2015":"https://www.fsa.go.jp/search/20150310/1c.zip",
            "2014":"https://www.fsa.go.jp/search/20140310/1c.zip"
        }
        
        r = requests.get(download_link_dict[self.account_list_year], stream=True)
        with self.taxonomy_file.open(mode="wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

    def _download_jpcrp_lab(self):
        already_download_list = list(self.taxonomy_path.glob("jpcrp_{}_lab.xml".format(self.account_list_year)))
        if len(already_download_list)>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab.xml" in item) & ("jpcrp" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path = list(self.temp_path.glob("**/*.xml"))[0]
            f_path = f_path.rename(self.taxonomy_path/f_path.name)
            return f_path

    def _download_jpcrp_lab_en(self):
        already_download_list = list(self.taxonomy_path.glob("jpcrp_{}_lab-en.xml".format(self.account_list_year)))
        if len(already_download_list)>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab-en.xml" in item) & ("jpcrp" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path = list(self.temp_path.glob("**/*.xml"))[0]
            f_path = f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def _download_jppfs_lab(self):
        already_download_list = list(self.taxonomy_path.glob("jppfs_{}_lab.xml".format(self.account_list_year)))
        if len(already_download_list)>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab.xml" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path = list(self.temp_path.glob("**/*.xml"))[0]
            f_path = f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def _download_jppfs_lab_en(self):
        already_download_list=list(self.taxonomy_path.glob("jpcrp_{}_lab-en.xml".format(self.account_list_year)))
        if len(already_download_list)>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab-en.xml" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path = list(self.temp_path.glob("**/*.xml"))[0]
            f_path = f_path.rename(self.taxonomy_path/f_path.name)
            return f_path

    def _download_jpcrp_pre(self):
        already_download_list=list(self.taxonomy_path.glob("jpcrp030000-asr_{}_pre.xml".format(self.account_list_year)))
        if len(already_download_list)>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("pre.xml" in item) & ("jpcrp030000-asr" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path = list(self.temp_path.glob("**/*.xml"))[0]
            f_path = f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def _download_jppfs_pre(self)->list:
        already_download_list=list(self.taxonomy_path.glob("jppfs*_pre_*.xml"))
        
        if len(already_download_list)>500: # 652 files in 2024
            #print("already_download_list: ",len(already_download_list))
            return already_download_list
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("_pre_" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    for f in fn:
                        zf.extract(f, self.temp_path)
            f_path_new_list = []
            for f_path in list(self.temp_path.glob("**/*.xml")):
                f_path_new = f_path.rename(self.taxonomy_path/f_path.name)
                f_path_new_list.append(f_path_new)
            #print("{} files are downloaded".format(len(f_path_new_list)))
            return f_path_new_list
    
    def _build(self):
        self.get_label_common_obj_jpcrp_lab = get_label_common(
            file_str=self.path_jpcrp_lab,
            lang="Japanese"
            )
        self.get_label_common_obj_jpcrp_lab_en = get_label_common(
            file_str=self.path_jpcrp_lab_en,
            lang="English"
            )
        self.get_label_common_obj_jppfs_lab = get_label_common(
            file_str=self.path_jppfs_lab,
            lang="Japanese"
            )
        self.get_label_common_obj_jppfs_lab_en = get_label_common(
            file_str=self.path_jppfs_lab_en,
            lang="English"
            )
        
        self.get_presentation_common_obj = get_presentation_common(
            file_str=self.path_jpcrp_pre
        )
        self.label_to_taxonomi_dict = self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        
        for path in self.path_jppfs_pre_list:
            get_presentation_common_obj = get_presentation_common(
                file_str=path
            )
            self.label_to_taxonomi_dict.update(get_presentation_common_obj.export_label_to_taxonomi_dict())
        
        self.assign_common_label(short_label_only=False)

    def get_assign_common_label(self):
        return self.assign_common_label_df
    
    def assign_common_label(self,short_label_only=True):
        """
            TODO: keyでユニークにしているため、同じkeyが複数ある場合は、最初のものが残る結果、別のLabelが紐づく可能性がある
        """
        #label_to_taxonomi_dict = self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        label_tbl_jpcrp_jp = self.get_label_common_obj_jpcrp_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict
            )
        df_jpcrp = label_tbl_jpcrp_jp.query("role == 'label'").drop_duplicates(subset='key').set_index("key").rename(columns={"text":"label_jp"})
        if not short_label_only:
            get_label_common_obj = get_label_common(
                file_str=self.path_jpcrp_lab_en,
                lang="English"
                )
            label_tbl_jpcrp_en = self.get_label_common_obj_jpcrp_lab_en.export_label_tbl(
                label_to_taxonomi_dict=self.label_to_taxonomi_dict
            )
            df_jpcrp = df_jpcrp.join([
                label_tbl_jpcrp_jp.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_jp_long"}),
                label_tbl_jpcrp_en.query("role == 'label'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_en"}),
                label_tbl_jpcrp_en.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_en_long"})
                ],how="left")
        
        label_tbl_jppfs_jp = self.get_label_common_obj_jppfs_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict
        )
        df_jppfs = label_tbl_jppfs_jp.query("role == 'label'").drop_duplicates(subset='key').set_index("key").rename(columns={"text":"label_jp"})
        
        if not short_label_only:
            label_tbl_jppfs_en = self.get_label_common_obj_jppfs_lab_en.export_label_tbl(
                label_to_taxonomi_dict=self.label_to_taxonomi_dict
            )
            df_jppfs = df_jppfs.join([
                label_tbl_jppfs_jp.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_jp_long"}),
                label_tbl_jppfs_en.query("role == 'label'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_en"}),
                label_tbl_jppfs_en.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_en_long"})
                ],how="left")
        self.assign_common_label_df = pd.concat([df_jpcrp,df_jppfs]).drop_duplicates()



class get_presentation_common():
    """
    """
    def __init__(self,file_str:str):#->(parent_child_link_schima,,dict,dict):
        
        self.log_dict = {
            #'docID':docid,
            'is_pre_file_flg':None,
            'get_pre_status':None,
            'get_pre_error_message':None
            }
        self.file_path=Path(file_str)
        self.parse_pre_file()
    
    
    def parse_pre_file(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        locators = []
        arcs = []
        for child in root:
            attr_sr_p = pd.Series(child.attrib)
            role = attr_sr_p[attr_sr_p.index.str.contains('role')].item()
            for child_of_child in child:
                locator = {'role':role,'schima_taxonomi':None,'label':None,'schima_taxonomi_head':None}
                arc = {'parent':None,'child':None,'child_order':None,'role':role}

                attr_sr = pd.Series(child_of_child.attrib)
                attr_type = attr_sr[attr_sr.index.str.contains('type')].item()
                if attr_type=='locator':
                    locator['schima_taxonomi_head'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[0]
                    locator['schima_taxonomi'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                    locator['label'] = attr_sr[attr_sr.index.str.contains('label')].item()
                    locators.append(PreLocator(**locator))
                elif attr_type=='arc':
                    arc['parent'] = attr_sr[attr_sr.index.str.contains('from')].item()
                    arc['child'] = attr_sr[attr_sr.index.str.contains('to')].item()
                    arc['child_order'] = '1'#attr_sr[attr_sr.index.str.contains('order')].item()
                    arcs.append(Arc(**arc))
                
        self.locators = locators
        self.arcs = arcs

    def _make_label_to_taxonomi_dict(self)->dict:
        
        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
            )
        self.label_to_taxonomi_dict = locators_df.set_index('label')['key'].to_dict()

    def export_account_list_df(self)->OriginalAccountList:
        locators_df = pd.DataFrame([locator.model_dump() for locator in self.locators]).dropna(subset=['schima_taxonomi'])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi)
                                    )
        pre_detail_list = OriginalAccountList(locators_df[get_columns_df(OriginalAccountList)])
        return pre_detail_list
    def export_parent_child_link_df(self)->ParentChildLink:
        self._make_label_to_taxonomi_dict()
        arcs_df = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(subset=['child'])
        arcs_df = arcs_df.assign(
            parent_key = arcs_df.parent.replace(self.label_to_taxonomi_dict),
            child_key = arcs_df.child.replace(self.label_to_taxonomi_dict))

        arcs_df = ParentChildLink(arcs_df[get_columns_df(ParentChildLink)])
        return arcs_df
    
    def export_log(self)->GetPresentationLog:
        return GetPresentationLog(**self.log_dict)
    
    def export_label_to_taxonomi_dict(self):
        self._make_label_to_taxonomi_dict()
        return self.label_to_taxonomi_dict

# %% #################################################################
#
#            deprecated
#
######################################################################

def get_presentation_account_list_aud(docid:str,identifier:str,out_path)->(ParentChildLink,OriginalAccountList,dict,dict):
    """
    locator:
        (role:)
        href:
        label:
    arc:
        (role:)
        from:
        to:
        order:
        role is given to edge
    """
    dict_t={
        'docID':docid,
        'org_taxonomi_cnt':None,
        'org_taxonomi_list':[],
        'status':None,
        'error_message':None
            }
    try:
        data_dir_raw = PROJDIR / "data" / "1_raw"
        zip_file = list(data_dir_raw.glob("data_pool_*/"+docid+".zip"))[0]
        with ZipFile(str(zip_file)) as zf:
                fn=[item for item in zf.namelist() if ("pre.xml" in item)&("aai" in item)]
                if len(fn)>0:
                    zf.extract(fn[0], out_path)
        xml_def_path=out_path / "XBRL" / "AuditDoc"
        if len(list(xml_def_path.glob("*pre.xml")))==0:
            raise Exception("No pre.xml file")
        else:
            tree = ET.parse(str(list(xml_def_path.glob("*pre.xml"))[0]))
            root = tree.getroot()
    
            locators = []
            arcs = []
            for child in root:
                attr_sr_p = pd.Series(child.attrib)
                role = attr_sr_p[attr_sr_p.index.str.contains('role')].item()
                for child_of_child in child:
                    locator = {'role':role,'schima_taxonomi':None}
                    arc = {'parent':None,'child':None,'child_order':None,'role':role}
    
                    attr_sr = pd.Series(child_of_child.attrib)
                    attr_type = attr_sr[attr_sr.index.str.contains('type')].item()
                    if attr_type=='locator':
                        locator['schima_taxonomi'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                        locator['label'] = attr_sr[attr_sr.index.str.contains('label')].item()
                    elif attr_type=='arc':
                        arc['parent'] = attr_sr[attr_sr.index.str.contains('from')].item()
                        arc['child'] = attr_sr[attr_sr.index.str.contains('to')].item()
                        arc['child_order'] = attr_sr[attr_sr.index.str.contains('order')].item()
    
                    locators.append(locator)
                    arcs.append(arc)
    
            locators_df = pd.DataFrame(locators).dropna(subset=['schima_taxonomi'])
            locators_df = locators_df.assign(
                role=locators_df.role.str.split('/',expand=True).iloc[:,-1],
                key=locators_df.schima_taxonomi.apply(format_taxonomi)
                                           )
            label_to_taxonomi_dict = locators_df.set_index('label')['key'].to_dict()
    
            p_edges_df = pd.DataFrame(arcs).dropna(subset=['child'])
            p_edges_df = p_edges_df.assign(
                parent_key=p_edges_df.parent.replace(label_to_taxonomi_dict),
                child_key = p_edges_df.child.replace(label_to_taxonomi_dict))
    
            p_edges_df = ParentChildLink(p_edges_df)
            pre_detail_list = OriginalAccountList(locators_df)
            dict_t['status'] = 'success'            
            #dict_t['org_taxonomi_cnt'] = len(pre_detail_list.query("schima_taxonomi.str.contains(@identifier)"))
            #dict_t['org_taxonomi_list'] = pre_detail_list.query("schima_taxonomi.str.contains(@identifier)").schima_taxonomi.to_list()
    except Exception as e:
        #print(e)
        label_to_taxonomi_dict = {}
        dict_t['status'] = 'error'
        dict_t['error_message'] = e
        p_edges_df = ParentChildLink(pd.DataFrame(columns=get_columns_df(ParentChildLink)))
        pre_detail_list = OriginalAccountList(pd.DataFrame(columns=get_columns_df(OriginalAccountList)))
    
    return p_edges_df[get_columns_df(ParentChildLink)],pre_detail_list[get_columns_df(OriginalAccountList)],label_to_taxonomi_dict,dict_t

