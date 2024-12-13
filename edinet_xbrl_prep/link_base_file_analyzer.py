
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

def get_columns_df(schima:pa.DataFrameModel)->list:
    return list(schima.to_schema().columns.keys())


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

def format_taxonomi(taxonomi_str:str)->str:
    """
    Convert
        From:
        jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF
        To:
        jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF
    """
    return "_".join(taxonomi_str.split('_')[:-1])+":"+taxonomi_str.split('_')[-1]
# %% #################################################################
#
#            account_link_tracer
#
######################################################################

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


class fs_tbl_loader():
    def __init__(self,account_list_common_obj,docid,zip_file_str,temp_path_str,role_to_get):
        self.linkbasefile_obj = linkbasefile(
            zip_file_str=zip_file_str,
            temp_path_str=temp_path_str
            )
        with timer("read_linkbase_file"):
            self.linkbasefile_obj.read_linkbase_file()
        self.linkbasefile_obj.check()
        self.linkbasefile_obj.make_account_label(account_list_common_obj,role_to_get)
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
        

    def get_data(self,doc_name='BS',term='current',Consolidated=True):
        assert doc_name in ['BS','PL','CF','SS','NOTES'],"doc_name should be one of ['BS','PL','CF','SS','NOTES']"
        assert term in ['current','prior','all'],"term should be one of ['current','prior','all']"
        assert isinstance(Consolidated,bool),"Consolidated should be boolean"
    
        fs_dict={
            'BS':"_BalanceSheet",
            'PL':"_StatementOfIncome",
            'CF':"_StatementOfCashFlows",
            'SS':"_StatementOfChangesInEquity",
            'NOTES':"_Notes"}

        role_list = [role_key for role_key in list(self.linkbasefile_obj.account_tbl_role_dict.keys()) if fs_dict[doc_name] in role_key]
        data_list = []
        for role in role_list:
            key_in_the_role = self.linkbasefile_obj.account_tbl_role_dict[role].key
            print(len(key_in_the_role))
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
            data = data.assign(docID=self.docid)
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

    def make_account_label(self,account_list_common_obj,role_to_get_list=['BS','PL','CF','SS','NOTES']):
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
                'BS':"_BalanceSheet",
                'PL':"_StatementOfIncome",
                'CF':"_StatementOfCashFlows",
                'SS':"_StatementOfChangesInEquity",
                'NOTES':"_Notes"}
            role_list = list(set(self.parent_child_df.role))
            role_list_f = []
            if role_to_get_list:
                for role_to_get in role_to_get_list:
                    role_list_f = role_list_f + [role_key for role_key in role_list if fs_dict[role_to_get] in role_key]
            else:
                role_list_f = role_list
            
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
        print("label: ",len(label_tbl))
        label_tbl=AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace('label_',''),
                key=label_tbl.key_all.replace(label_to_taxonomi_dict)
                )[get_columns_df(AccountLabel)]
            )
        return label_tbl


class account_list_common():
    def __init__(self,data_path:str,account_list_year:str):

        linkfiles_dict={
            'pre.xml':"jpcrp030000-asr",
            'lab.xml':"jpcrp",
            'lab-en.xml':"jpcrp"}
        schima_word_list=['jppfs','jpcrp']
        self.taxonomy_file=data_path / "taxonomy_{}.zip".format(account_list_year)
        self.account_list_year=account_list_year
        self.temp_path=data_path / "tmp/taxonomy"
        self.temp_path.mkdir(parents=True, exist_ok=True)
        self.taxonomy_path=data_path / "taxonomy"
        self.taxonomy_path.mkdir(parents=True, exist_ok=True)
        self.download_taxonomy()
        self.path_jpcrp_lab = self.download_jpcrp_lab()
        self.path_jpcrp_lab_en = self.download_jpcrp_lab_en()
        self.path_jppfs_lab = self.download_jppfs_lab()
        self.path_jppfs_lab_en = self.download_jppfs_lab_en()
        self.path_jpcrp_pre = self.download_jpcrp_pre()
        self.path_jppfs_pre_list = self.download_jppfs_pre()
        self.build()

    def download_taxonomy(self):
        download_link_dict = {
            '2024':"https://www.fsa.go.jp/search/20231211/1c_Taxonomy.zip",
            "2023":"https://www.fsa.go.jp/search/20221108/1c_Taxonomy.zip",
            "2022":"https://www.fsa.go.jp/search/20211109/1c_Taxonomy.zip",
            "2021":"https://www.fsa.go.jp/search/20201110/1c_Taxonomy.zip",
            "2020":"https://www.fsa.go.jp/search/20191101/1c_Taxonomy.zip"
        }
        
        r = requests.get(download_link_dict[self.account_list_year], stream=True)
        with self.taxonomy_file.open(mode="wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

    def download_jpcrp_lab(self):
        already_download_list=self.taxonomy_path.glob("jpcrp_{}_lab.xml".format(self.account_list_year))
        if len(list(already_download_list))>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab.xml" in item) & ("jpcrp" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path=list(self.temp_path.glob("**/*.xml"))[0]
            f_path=f_path.rename(self.taxonomy_path/f_path.name)
            return f_path

    def download_jpcrp_lab_en(self):
        already_download_list=self.taxonomy_path.glob("jpcrp_{}_lab-en.xml".format(self.account_list_year))
        if len(list(already_download_list))>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab-en.xml" in item) & ("jpcrp" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path=list(self.temp_path.glob("**/*.xml"))[0]
            f_path=f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def download_jppfs_lab(self):
        already_download_list=self.taxonomy_path.glob("jppfs_{}_lab.xml".format(self.account_list_year))
        if len(list(already_download_list))>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab.xml" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path=list(self.temp_path.glob("**/*.xml"))[0]
            f_path=f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def download_jppfs_lab_en(self):
        already_download_list=self.taxonomy_path.glob("jpcrp_{}_lab-en.xml".format(self.account_list_year))
        if len(list(already_download_list))>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("lab-en.xml" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path=list(self.temp_path.glob("**/*.xml"))[0]
            f_path=f_path.rename(self.taxonomy_path/f_path.name)
            return f_path

    def download_jpcrp_pre(self):
        already_download_list=self.taxonomy_path.glob("jpcrp030000-asr_{}_pre.xml".format(self.account_list_year))
        if len(list(already_download_list))>0:
            return already_download_list[0]
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("pre.xml" in item) & ("jpcrp030000-asr" in item) & ("dep" not in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            f_path=list(self.temp_path.glob("**/*.xml"))[0]
            f_path=f_path.rename(self.taxonomy_path/f_path.name)
            return f_path
    
    def download_jppfs_pre(self)->list:
        already_download_list=self.taxonomy_path.glob("jppfs_*pre*.xml")
        if len(list(already_download_list))>9:
            return already_download_list
        else:
            with ZipFile(str(self.taxonomy_file)) as zf:
                fn=[item for item in zf.namelist() if ("_pre_" in item) & ("jppfs" in item) & ("dep" not in item)]
                if len(fn)>0:
                    for f in fn:
                        zf.extract(f, self.temp_path)
            f_path_new_list = []
            for f_path in list(self.temp_path.glob("**/*.xml")):
                f_path_new=f_path.rename(self.taxonomy_path/f_path.name)
                f_path_new_list.append(f_path_new)
            return f_path_new_list
    
    def build(self):
        self.get_label_common_obj_jpcrp_lab = get_label_common(
            file_str=self.path_jpcrp_lab,
            lang="Japanese"
            )
        self.get_label_common_obj_jpcrp_lab_en = get_label_common(
            file_str=self.path_jpcrp_lab,
            lang="English"
            )
        self.get_label_common_obj_jppfs_lab = get_label_common(
            file_str=self.path_jppfs_lab,
            lang="Japanese"
            )
        self.get_label_common_obj_jppfs_lab_en = get_label_common(
            file_str=self.path_jppfs_lab,
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
        
        self.assign_common_label()

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
                label_tbl_jpcrp_en.query("role == 'label'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_eng"}),
                label_tbl_jpcrp_en.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_eng_long"})
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
                label_tbl_jppfs_en.query("role == 'label'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_eng"}),
                label_tbl_jppfs_en.query("role == 'verboseLabel'").drop_duplicates(subset='key').set_index("key")[['text']].rename(columns={"text":"label_eng_long"})
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


# memo

def get_presentation_account_list2(zip_file_path,temp_path_str,doc_type='public')->(ParentChildLink,OriginalAccountList,dict,dict):
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
    log_dict = {
        #'docID':docid,
        'org_taxonomi_cnt':None,
        'org_taxonomi_list':[],
        'status':None,
        'error_message':None
            }
    temp_path=Path(temp_path_str)
    temp_path.mkdir(parents=True,exist_ok=True)

    if doc_type == 'audit':
        doc_type_str = 'aai'
        xml_def_path = temp_path / "XBRL" / "AuditDoc"
    elif doc_type == 'public':
        doc_type_str = 'asr'
        xml_def_path = temp_path / "XBRL" / "PublicDoc"
    else:
        raise Exception("doc_type must be 'audit' or 'public'")

    try:
        with ZipFile(str(zip_file_path)) as zf:
            fn=[item for item in zf.namelist() if ("pre.xml" in item) & (doc_type_str in item)]
            if len(fn)>0:
                zf.extract(fn[0], temp_path)
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
            
            arcs_df = pd.DataFrame(arcs).dropna(subset=['child'])
            arcs_df = arcs_df.assign(
                parent_key=arcs_df.parent.replace(label_to_taxonomi_dict),
                child_key = arcs_df.child.replace(label_to_taxonomi_dict))

            arcs_df = ParentChildLink(arcs_df)
            pre_detail_list = OriginalAccountList(locators_df)
            log_dict['get_pre_status'] = 'success'            
            log_dict['get_pre_error_message'] = None
        
    except Exception as e:
        #print(e)
        label_to_taxonomi_dict = {}
        log_dict['get_pre_status'] = 'error'
        log_dict['get_pre_error_message'] = e
        arcs_df = ParentChildLink(pd.DataFrame(columns=get_columns_df(ParentChildLink)))
        pre_detail_list = OriginalAccountList(pd.DataFrame(columns=get_columns_df(OriginalAccountList)))
        pass
    return arcs_df[get_columns_df(ParentChildLink)], pre_detail_list[get_columns_df(OriginalAccountList)], label_to_taxonomi_dict, GestPresentationLog(**log_dict)

def get_calc_edge_list2(zip_file,temp_path,log_dict=None):
    if log_dict is None:
        log_dict = {
            #'docID':docid,
            'org_taxonomi_cnt':None,
            'org_taxonomi_list':[],
            'status':None,
            'error_message':None
                }
    doc_type_str = 'asr'
    xml_def_path = temp_path / "XBRL" / "PublicDoc"
    try:
        #data_dir_raw = PROJDIR / "data" / "1_raw"
        #zip_file = list(data_dir_law.glob("data_pool_*/"+self.docid+".zip"))[0]
        with ZipFile(str(zip_file)) as zf:
            fn=[item for item in zf.namelist() if ("cal.xml" in item)&(doc_type_str in item)]
            if len(fn)>0:
                zf.extract(fn[0], temp_path)        
        tree = ET.parse(str(list(xml_def_path.glob("*cal.xml"))[0]))
        root = tree.getroot()

        locators=[]
        arcs=[]
        for child in root:
            attr_sr_p=pd.Series(child.attrib)
            role=attr_sr_p[attr_sr_p.index.str.contains('role')].item()
            for child_of_child in child:
                locator={'schima_taxonomi':None,'label':None,'fs':role}
                arc={'parent':None,'child':None,'child_order':None,'weight':None,'fs':role}
                attr_sr=pd.Series(child_of_child.attrib)
                attr_type=attr_sr[attr_sr.index.str.contains('type')].item() 
                if attr_type=='locator':
                    locator['schima_taxonomi']=attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                    locator['label']=attr_sr[attr_sr.index.str.contains('label')].item()

                elif attr_type=='arc':
                    arc['parent']=attr_sr[attr_sr.index.str.contains('from')].item()
                    arc['child']=attr_sr[attr_sr.index.str.contains('to')].item()
                    arc['child_order']=attr_sr[attr_sr.index.str.contains('order')].item()
                    arc['weight']=attr_sr[attr_sr.index.str.contains('weight')].item()

                locators.append(locator)
                arcs.append(arc)


        locators_df=pd.DataFrame(locators).dropna(subset=['schima_taxonomi'])
        locators_df=locators_df.assign(rol=locators_df.fs.str.split('/',expand=True).iloc[:,-1],
                                        key=locators_df.schima_taxonomi.apply(format_taxonomi))
        label_to_taxonomi_dict=locators_df.set_index('label')['key'].to_dict()
        arcs_df=pd.DataFrame(arcs).dropna(subset=['child'])
        arcs_df=arcs_df.assign(
            parent_key=arcs_df.parent.replace(label_to_taxonomi_dict),
            child_key=arcs_df.child.replace(label_to_taxonomi_dict),
            weight=arcs_df.weight.astype(float))
        
        arcs_df=CalParentChildLink(arcs_df.drop_duplicates(subset=['parent_taxonomi_tag','child_taxonomi_tag']))
        log_dict['load_pre']='success'
    except Exception as e:
        print(e)
        log_dict['load_pre']=e
        arcs_df=pd.DataFrame(columns=['parent_taxonomi_tag','child_taxonomi_tag','weight','fs'])


def get_label2(self,lang:str='English')->pd.DataFrame:
    if lang=='Japanese':
        f_keyword='lab.xml'
    else:
        f_keyword='lab-en.xml'
    try:
        zip_file = list(self.data_dir_law.glob("data_pool_*/"+self.docid+".zip"))[0]
        with ZipFile(str(zip_file)) as zf:
                fn=[item for item in zf.namelist() if f_keyword in item]
                if len(fn)>0:
                    zf.extract(fn[0], self.out_path)
        xml_def_path=self.out_path / "XBRL" / "PublicDoc"
        tree = ET.parse(str(list(xml_def_path.glob("*"+f_keyword))[0])) # TODO check iregular file name
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
                elif attr_type=='arc':
                    arc['label_pre']=attr_sr[attr_sr.index.str.contains('from')].values[0]
                    arc['label_lab']=attr_sr[attr_sr.index.str.contains('to')].values[0]
                resources.append(resource)
                arcs.append(arc)
        label_to_prelabel_dict=pd.DataFrame(arcs).dropna(subset='label_lab').set_index('label_lab')['label_pre'].to_dict()
        
        label_tbl=pd.DataFrame(resources).dropna(subset='label_lab')
        label_tbl=label_tbl.assign(label_taxonomi_tag=label_tbl.label_lab.replace(label_to_prelabel_dict).replace(self.label_to_taxonomi_dict))
                    
        #label_tbl=pd.merge(pd.DataFrame(resources).dropna(subset='label_lab'),pd.DataFrame(arcs).dropna(subset='label_lab'),left_on='label_lab',right_on='label_lab')
        #self.label_to_taxonomi_dict
        self.proc_rst['load_label']='success'

        return label_tbl
    except Exception as e:
        #print(e)
        self.proc_rst['load_label']=e
        return pd.DataFrame(columns=['label_lab','label_pre','lang','role','text'])




class get_def_list():

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
                fn=[item for item in zf.namelist() if ("def.xml" in item) & (self.doc_type_str in item)]
                if len(fn)>0:
                    zf.extract(fn[0], self.temp_path)
            if len(list(self.xml_def_path.glob("*def.xml")))==0:
                self.log_dict['is_pre_file_flg'] = 0
                raise Exception("No def.xml file")
            else:
                self.log_dict['is_pre_file_flg'] = 1
            self.log_dict['get_pre_status'] = 'success'
            
        except Exception as e:
            self.log_dict['is_pre_file_flg'] = 0
            self.log_dict['get_pre_status'] = 'failure'
            self.log_dict['get_pre_error_message'] = str(e)
    
    def parse_pre_file(self):
        tree = ET.parse(str(list(self.xml_def_path.glob("*def.xml"))[0]))
        root = tree.getroot()
        locators = []
        arcs = []
        for child in root:
            attr_sr_p = pd.Series(child.attrib)
            role = attr_sr_p[attr_sr_p.index.str.contains('role')].item()
            for child_of_child in child:
                locator = {'role':role,'schima_taxonomi':None,'label':None}
                arc = {'parent':None,'child':None,'child_order':None,'role':role}

                attr_sr = pd.Series(child_of_child.attrib)
                attr_type = attr_sr[attr_sr.index.str.contains('type')].item()
                if attr_type=='locator':
                    locator['schima_taxonomi'] = attr_sr[attr_sr.index.str.contains('href')].item().split('#')[1]
                    locator['label'] = attr_sr[attr_sr.index.str.contains('label')].item()
                    locators.append(Locator(**locator))
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