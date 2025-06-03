"""リンクベースファイル解析用モジュール"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Annotated
from zipfile import ZipFile

import pandas as pd
import requests
from pydantic.functional_validators import BeforeValidator

from .link_base_file_analyzer import *
from .utils import *
from .xbrl_parser_rapper import *

# %%
StrOrNone = Annotated[str, BeforeValidator(lambda x: x or "")]
FloatOrNone = Annotated[float, BeforeValidator(lambda x: x or 0.0)]


# %% common


class GetLabelCommon:
    def __init__(self, file_str: str, lang: str = "English") -> pd.DataFrame:
        self.log_dict = {
            "is_lab_file_flg": 1,
            "get_lab_status": "success",
            "get_lab_error_message": None,
        }
        self.file_path = Path(file_str)
        self.lang = lang
        if lang == "Japanese":
            self.f_keyword = "lab.xml"
        else:
            self.f_keyword = "lab-en.xml"
        # self.xml_def_path = self.temp_path / "XBRL" / "PublicDoc"
        # self.extruct_lab_file_from_xbrlzip(zip_file_str)
        if self.log_dict["get_lab_status"] != "failure":
            self.parse_lab_file()
            self._make_label_to_taxonomi_dict()

    # def extruct_lab_file_from_xbrlzip(self,zip_file_str:Path):
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
        tree = ET.parse(self.file_path)  # TODO check iregular file name
        root = tree.getroot()

        resources = []
        arcs = []
        for child in root:
            for child_of_child in child:
                resource = {"label_lab": None, "lang": None, "role": None, "text": None}
                arc = {"label_pre": None, "label_lab": None}
                attr_sr = pd.Series(child_of_child.attrib)
                attr_type = attr_sr[attr_sr.index.str.contains("type")].values[0]
                if attr_type == "resource":
                    resource["label_lab"] = attr_sr[
                        attr_sr.index.str.contains("label")
                    ].values[0]
                    resource["lang"] = attr_sr[
                        attr_sr.index.str.contains("lang")
                    ].values[0]
                    resource["role"] = (
                        attr_sr[attr_sr.index.str.contains("role")]
                        .values[0]
                        .split("/")[-1]
                    )
                    resource["text"] = child_of_child.text
                    resources.append(Resource(**resource))
                elif attr_type == "arc":
                    arc["label_pre"] = attr_sr[
                        attr_sr.index.str.contains("from")
                    ].values[0]
                    arc["label_lab"] = attr_sr[attr_sr.index.str.contains("to")].values[
                        0
                    ]
                    arcs.append(LabArc(**arc))

        self.resources = resources
        self.arcs = arcs

    def _make_label_to_taxonomi_dict(self):
        self.label_to_prelabel_dict = (
            pd.DataFrame([arc.model_dump() for arc in self.arcs])
            .dropna(subset="label_lab")
            .set_index("label_lab")["label_pre"]
            .to_dict()
        )
        label_tbl = pd.DataFrame(
            [resource.model_dump() for resource in self.resources],
        ).dropna(subset="label_lab")
        self.label_tbl = label_tbl.assign(
            key_all=label_tbl.label_lab.replace(
                self.label_to_prelabel_dict,
            ),  # .replace(label_to_taxonomi_dict)
        )

    def export_label_tbl2(self, label_to_taxonomi_dict: dict) -> pd.DataFrame:
        self._make_label_to_taxonomi_dict()
        label_tbl = pd.DataFrame(
            [resource.model_dump() for resource in self.resources],
        ).dropna(subset="label_lab")
        label_tbl = label_tbl.assign(
            key_all=label_tbl.label_lab.replace(
                self.label_to_prelabel_dict,
            ),  # .replace(label_to_taxonomi_dict)
        ).query("key_all in @label_to_taxonomi_dict.keys()")
        label_tbl = AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace("label_", ""),
                key=label_tbl.key_all.replace(label_to_taxonomi_dict),
            ),  # [get_columns_df(AccountLabel)]
        )
        return label_tbl

    def export_label_tbl(self, label_to_taxonomi_dict: dict) -> pd.DataFrame:
        """TODO: change label to taxonomi"""
        label_tbl = self.label_tbl.query("key_all in @label_to_taxonomi_dict.keys()")
        # print("label: ",len(label_tbl))
        label_tbl = AccountLabel(
            label_tbl.assign(
                label=label_tbl.label_lab.str.replace("label_", ""),
                key=label_tbl.key_all.replace(label_to_taxonomi_dict),
            )[get_columns_df(AccountLabel)],
        )
        return label_tbl


class AccountListCommon:
    """共通タクソノミの取得。主にリンクベースファイルでimportされているlabel情報を取得する。"""

    def __init__(self, data_path: str, account_list_year: str):
        linkfiles_dict = {
            "pre.xml": "jpcrp030000-asr",
            "lab.xml": "jpcrp",
            "lab-en.xml": "jpcrp",
        }
        schima_word_list = ["jppfs", "jpcrp"]
        self.taxonomy_file = data_path / f"taxonomy_{account_list_year}.zip"
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
        if self.account_list_year in ["2019", "2020", "2021", "2022", "2023", "2024"]:
            self.path_jpigp_lab = self._download_jpigp_lab()
            self.path_jpigp_lab_en = self._download_jpigp_lab_en()
            self.path_jpigp_pre_list = self._download_jpigp_pre()

        self._build()

    def show_m(self):
        return [meth for meth in dir(account_list_common_obj_2014) if meth[0:2] != "__"]

    def _download_taxonomy(self):
        download_link_dict = {
            "2024": "https://www.fsa.go.jp/search/20231211/1c_Taxonomy.zip",
            "2023": "https://www.fsa.go.jp/search/20221108/1c_Taxonomy.zip",
            "2022": "https://www.fsa.go.jp/search/20211109/1c_Taxonomy.zip",
            "2021": "https://www.fsa.go.jp/search/20201110/1c_Taxonomy.zip",
            "2020": "https://www.fsa.go.jp/search/20191101/1c_Taxonomy.zip",
            "2019": "https://www.fsa.go.jp/search/20190228/1c_Taxonomy.zip",
            "2018": "https://www.fsa.go.jp/search/20180228/1c_Taxonomy.zip",
            "2017": "https://www.fsa.go.jp/search/20170228/1c.zip",
            "2016": "https://www.fsa.go.jp/search/20160314/1c.zip",
            "2015": "https://www.fsa.go.jp/search/20150310/1c.zip",
            "2014": "https://www.fsa.go.jp/search/20140310/1c.zip",
        }

        r = requests.get(download_link_dict[self.account_list_year], stream=True)
        with self.taxonomy_file.open(mode="wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

    def _download_jpcrp_lab(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab.xml" in item) & ("jpcrp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpcrp_lab_en(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab-en.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab-en.xml" in item) & ("jpcrp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_lab(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jppfs_{self.account_list_year}_lab.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab.xml" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_lab_en(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab-en.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab-en.xml" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpigp_lab(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpigp_{self.account_list_year}_lab.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab.xml" in item) & ("jpigp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpigp_lab_en(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab-en.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab-en.xml" in item) & ("jpigp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpcrp_pre(self):
        already_download_list = list(
            self.taxonomy_path.glob(
                f"jpcrp030000-asr_{self.account_list_year}_pre.xml",
            ),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("pre.xml" in item)
                & ("jpcrp030000-asr" in item)
                & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_pre(self) -> list:
        already_download_list = list(self.taxonomy_path.glob("jppfs*_pre_*.xml"))

        if len(already_download_list) > 500:  # 652 files in 2024
            # print("already_download_list: ",len(already_download_list))
            return already_download_list
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("_pre_" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                for f in fn:
                    zf.extract(f, self.temp_path)
        f_path_new_list = []
        for f_path in list(self.temp_path.glob("**/*.xml")):
            f_path_new = f_path.rename(self.taxonomy_path / f_path.name)
            f_path_new_list.append(f_path_new)
        # print("{} files are downloaded".format(len(f_path_new_list)))
        return f_path_new_list

    def _download_jpigp_pre(self) -> list:
        already_download_list = list(self.taxonomy_path.glob("jpigp*_pre_*.xml"))

        if len(already_download_list) > 50:
            # print("already_download_list: ",len(already_download_list))
            return already_download_list
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("_pre_" in item) & ("jpigp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                for f in fn:
                    zf.extract(f, self.temp_path)
        f_path_new_list = []
        for f_path in list(self.temp_path.glob("**/*.xml")):
            f_path_new = f_path.rename(self.taxonomy_path / f_path.name)
            f_path_new_list.append(f_path_new)
        # print("{} files are downloaded".format(len(f_path_new_list)))
        return f_path_new_list

    def _download_jppfs_cal(self) -> list:
        already_download_list = list(self.taxonomy_path.glob("jppfs*_cal_*.xml"))

        if len(already_download_list) > 50:  # 652 files in 2024
            # print("already_download_list: ",len(already_download_list))
            return already_download_list
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("_cal_" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                for f in fn:
                    zf.extract(f, self.temp_path)
        f_path_new_list = []
        for f_path in list(self.temp_path.glob("**/*.xml")):
            f_path_new = f_path.rename(self.taxonomy_path / f_path.name)
            f_path_new_list.append(f_path_new)
        # print("{} files are downloaded".format(len(f_path_new_list)))
        return f_path_new_list

    def _build(self):
        self.get_label_common_obj_jpcrp_lab = get_label_common(
            file_str=self.path_jpcrp_lab,
            lang="Japanese",
        )
        self.get_label_common_obj_jpcrp_lab_en = get_label_common(
            file_str=self.path_jpcrp_lab_en,
            lang="English",
        )
        self.get_label_common_obj_jppfs_lab = get_label_common(
            file_str=self.path_jppfs_lab,
            lang="Japanese",
        )
        self.get_label_common_obj_jppfs_lab_en = get_label_common(
            file_str=self.path_jppfs_lab_en,
            lang="English",
        )
        if self.account_list_year in ["2019", "2020", "2021", "2022", "2023", "2024"]:
            self.get_label_common_obj_jpigp_lab = get_label_common(
                file_str=self.path_jpigp_lab,
                lang="Japanese",
            )
            self.get_label_common_obj_jpigp_lab_en = get_label_common(
                file_str=self.path_jpigp_lab_en,
                lang="English",
            )

        self.get_presentation_common_obj = get_presentation_common(
            file_str=self.path_jpcrp_pre,
        )
        self.label_to_taxonomi_dict = (
            self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        )

        for path in self.path_jppfs_pre_list:
            get_presentation_common_obj = get_presentation_common(
                file_str=path,
            )
            self.label_to_taxonomi_dict.update(
                get_presentation_common_obj.export_label_to_taxonomi_dict(),
            )

        if self.account_list_year in ["2019", "2020", "2021", "2022", "2023", "2024"]:
            for path in self.path_jpigp_pre_list:
                get_presentation_common_obj = get_presentation_common(
                    file_str=path,
                )
            self.label_to_taxonomi_dict.update(
                get_presentation_common_obj.export_label_to_taxonomi_dict(),
            )

        self.assign_common_label(short_label_only=False)

    def get_assign_common_label(self):
        return self.assign_common_label_df

    def assign_common_label(self, short_label_only=True):
        """TODO: keyでユニークにしているため、同じkeyが複数ある場合は、最初のものが残る結果、別のLabelが紐づく可能性がある"""
        # label_to_taxonomi_dict = self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        label_tbl_jpcrp_jp = self.get_label_common_obj_jpcrp_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict,
        )
        df_jpcrp = (
            label_tbl_jpcrp_jp.query("role == 'label'")
            .drop_duplicates(subset="key")
            .set_index("key")
            .rename(columns={"text": "label_jp"})
        )
        if not short_label_only:
            get_label_common_obj = get_label_common(
                file_str=self.path_jpcrp_lab_en,
                lang="English",
            )
            label_tbl_jpcrp_en = (
                self.get_label_common_obj_jpcrp_lab_en.export_label_tbl(
                    label_to_taxonomi_dict=self.label_to_taxonomi_dict,
                )
            )
            df_jpcrp = df_jpcrp.join(
                [
                    label_tbl_jpcrp_jp.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_jp_long"}),
                    label_tbl_jpcrp_en.query("role == 'label'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en"}),
                    label_tbl_jpcrp_en.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en_long"}),
                ],
                how="left",
            )

        label_tbl_jppfs_jp = self.get_label_common_obj_jppfs_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict,
        )
        df_jppfs = (
            label_tbl_jppfs_jp.query("role == 'label'")
            .drop_duplicates(subset="key")
            .set_index("key")
            .rename(columns={"text": "label_jp"})
        )

        if not short_label_only:
            label_tbl_jppfs_en = (
                self.get_label_common_obj_jppfs_lab_en.export_label_tbl(
                    label_to_taxonomi_dict=self.label_to_taxonomi_dict,
                )
            )
            df_jppfs = df_jppfs.join(
                [
                    label_tbl_jppfs_jp.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_jp_long"}),
                    label_tbl_jppfs_en.query("role == 'label'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en"}),
                    label_tbl_jppfs_en.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en_long"}),
                ],
                how="left",
            )
        self.assign_common_label_df = pd.concat([df_jpcrp, df_jppfs]).drop_duplicates()


class get_presentation_common:
    """ """

    def __init__(self, file_str: str):  # ->(parent_child_link_schima,,dict,dict):
        self.log_dict = {
            #'docID':docid,
            "is_pre_file_flg": None,
            "get_pre_status": None,
            "get_pre_error_message": None,
        }
        self.file_path = Path(file_str)
        self.parse_pre_file()

    def parse_pre_file(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        locators = []
        arcs = []
        for child in root:
            attr_sr_p = pd.Series(child.attrib)
            role = attr_sr_p[attr_sr_p.index.str.contains("role")].item()
            for child_of_child in child:
                locator = {
                    "role": role,
                    "schima_taxonomi": None,
                    "label": None,
                    "schima_taxonomi_head": None,
                }
                arc = {"parent": None, "child": None, "child_order": None, "role": role}

                attr_sr = pd.Series(child_of_child.attrib)
                attr_type = attr_sr[attr_sr.index.str.contains("type")].item()
                if attr_type == "locator":
                    locator["schima_taxonomi_head"] = (
                        attr_sr[attr_sr.index.str.contains("href")].item().split("#")[0]
                    )
                    locator["schima_taxonomi"] = (
                        attr_sr[attr_sr.index.str.contains("href")].item().split("#")[1]
                    )
                    locator["label"] = attr_sr[
                        attr_sr.index.str.contains("label")
                    ].item()
                    locators.append(PreLocator(**locator))
                elif attr_type == "arc":
                    arc["parent"] = attr_sr[attr_sr.index.str.contains("from")].item()
                    arc["child"] = attr_sr[attr_sr.index.str.contains("to")].item()
                    arc["child_order"] = (
                        "1"  # attr_sr[attr_sr.index.str.contains('order')].item()
                    )
                    arcs.append(Arc(**arc))

        self.locators = locators
        self.arcs = arcs

    def _make_label_to_taxonomi_dict(self) -> dict:
        locators_df = pd.DataFrame(
            [locator.model_dump() for locator in self.locators],
        ).dropna(subset=["schima_taxonomi"])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split("/", expand=True).iloc[:, -1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi),
        )
        self.label_to_taxonomi_dict = locators_df.set_index("label")["key"].to_dict()

    def export_account_list_df(self) -> OriginalAccountList:
        locators_df = pd.DataFrame(
            [locator.model_dump() for locator in self.locators],
        ).dropna(subset=["schima_taxonomi"])
        locators_df = locators_df.assign(
            role=locators_df.role.str.split("/", expand=True).iloc[:, -1],
            key=locators_df.schima_taxonomi.apply(format_taxonomi),
        )
        pre_detail_list = OriginalAccountList(
            locators_df[get_columns_df(OriginalAccountList)],
        )
        return pre_detail_list

    def export_parent_child_link_df(self) -> ParentChildLink:
        self._make_label_to_taxonomi_dict()
        arcs_df = pd.DataFrame([arc.model_dump() for arc in self.arcs]).dropna(
            subset=["child"],
        )
        arcs_df = arcs_df.assign(
            parent_key=arcs_df.parent.replace(self.label_to_taxonomi_dict),
            child_key=arcs_df.child.replace(self.label_to_taxonomi_dict),
        )

        arcs_df = ParentChildLink(arcs_df[get_columns_df(ParentChildLink)])
        return arcs_df

    def export_log(self) -> GetPresentationLog:
        return GetPresentationLog(**self.log_dict)

    def export_label_to_taxonomi_dict(self):
        self._make_label_to_taxonomi_dict()
        return self.label_to_taxonomi_dict


class account_list_common_old:
    """共通タクソノミの取得。主にリンクベースファイルでimportされているlabel情報を取得する。"""

    def __init__(self, data_path: str, account_list_year: str):
        linkfiles_dict = {
            "pre.xml": "jpcrp030000-asr",
            "lab.xml": "jpcrp",
            "lab-en.xml": "jpcrp",
        }
        schima_word_list = ["jppfs", "jpcrp"]
        self.taxonomy_file = data_path / f"taxonomy_{account_list_year}.zip"
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
            "2024": "https://www.fsa.go.jp/search/20231211/1c_Taxonomy.zip",
            "2023": "https://www.fsa.go.jp/search/20221108/1c_Taxonomy.zip",
            "2022": "https://www.fsa.go.jp/search/20211109/1c_Taxonomy.zip",
            "2021": "https://www.fsa.go.jp/search/20201110/1c_Taxonomy.zip",
            "2020": "https://www.fsa.go.jp/search/20191101/1c_Taxonomy.zip",
            "2019": "https://www.fsa.go.jp/search/20190228/1c_Taxonomy.zip",
            "2018": "https://www.fsa.go.jp/search/20180228/1c_Taxonomy.zip",
            "2017": "https://www.fsa.go.jp/search/20170228/1c.zip",
            "2016": "https://www.fsa.go.jp/search/20160314/1c.zip",
            "2015": "https://www.fsa.go.jp/search/20150310/1c.zip",
            "2014": "https://www.fsa.go.jp/search/20140310/1c.zip",
        }

        r = requests.get(download_link_dict[self.account_list_year], stream=True)
        with self.taxonomy_file.open(mode="wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

    def _download_jpcrp_lab(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab.xml" in item) & ("jpcrp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpcrp_lab_en(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab-en.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab-en.xml" in item) & ("jpcrp" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_lab(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jppfs_{self.account_list_year}_lab.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab.xml" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_lab_en(self):
        already_download_list = list(
            self.taxonomy_path.glob(f"jpcrp_{self.account_list_year}_lab-en.xml"),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("lab-en.xml" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jpcrp_pre(self):
        already_download_list = list(
            self.taxonomy_path.glob(
                f"jpcrp030000-asr_{self.account_list_year}_pre.xml"
            ),
        )
        if len(already_download_list) > 0:
            return already_download_list[0]
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("pre.xml" in item)
                & ("jpcrp030000-asr" in item)
                & ("dep" not in item)
            ]
            if len(fn) > 0:
                zf.extract(fn[0], self.temp_path)
        f_path = list(self.temp_path.glob("**/*.xml"))[0]
        f_path = f_path.rename(self.taxonomy_path / f_path.name)
        return f_path

    def _download_jppfs_pre(self) -> list:
        already_download_list = list(self.taxonomy_path.glob("jppfs*_pre_*.xml"))

        if len(already_download_list) > 500:  # 652 files in 2024
            # print("already_download_list: ",len(already_download_list))
            return already_download_list
        with ZipFile(str(self.taxonomy_file)) as zf:
            fn = [
                item
                for item in zf.namelist()
                if ("_pre_" in item) & ("jppfs" in item) & ("dep" not in item)
            ]
            if len(fn) > 0:
                for f in fn:
                    zf.extract(f, self.temp_path)
        f_path_new_list = []
        for f_path in list(self.temp_path.glob("**/*.xml")):
            f_path_new = f_path.rename(self.taxonomy_path / f_path.name)
            f_path_new_list.append(f_path_new)
        # print("{} files are downloaded".format(len(f_path_new_list)))
        return f_path_new_list

    def _build(self):
        self.get_label_common_obj_jpcrp_lab = get_label_common(
            file_str=self.path_jpcrp_lab,
            lang="Japanese",
        )
        self.get_label_common_obj_jpcrp_lab_en = get_label_common(
            file_str=self.path_jpcrp_lab_en,
            lang="English",
        )
        self.get_label_common_obj_jppfs_lab = get_label_common(
            file_str=self.path_jppfs_lab,
            lang="Japanese",
        )
        self.get_label_common_obj_jppfs_lab_en = get_label_common(
            file_str=self.path_jppfs_lab_en,
            lang="English",
        )

        self.get_presentation_common_obj = get_presentation_common(
            file_str=self.path_jpcrp_pre,
        )
        self.label_to_taxonomi_dict = (
            self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        )

        for path in self.path_jppfs_pre_list:
            get_presentation_common_obj = get_presentation_common(
                file_str=path,
            )
            self.label_to_taxonomi_dict.update(
                get_presentation_common_obj.export_label_to_taxonomi_dict(),
            )

        self.assign_common_label(short_label_only=False)

    def get_assign_common_label(self):
        return self.assign_common_label_df

    def assign_common_label(self, short_label_only=True):
        """TODO: keyでユニークにしているため、同じkeyが複数ある場合は、最初のものが残る結果、別のLabelが紐づく可能性がある"""
        # label_to_taxonomi_dict = self.get_presentation_common_obj.export_label_to_taxonomi_dict()
        label_tbl_jpcrp_jp = self.get_label_common_obj_jpcrp_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict,
        )
        df_jpcrp = (
            label_tbl_jpcrp_jp.query("role == 'label'")
            .drop_duplicates(subset="key")
            .set_index("key")
            .rename(columns={"text": "label_jp"})
        )
        if not short_label_only:
            get_label_common_obj = get_label_common(
                file_str=self.path_jpcrp_lab_en,
                lang="English",
            )
            label_tbl_jpcrp_en = (
                self.get_label_common_obj_jpcrp_lab_en.export_label_tbl(
                    label_to_taxonomi_dict=self.label_to_taxonomi_dict,
                )
            )
            df_jpcrp = df_jpcrp.join(
                [
                    label_tbl_jpcrp_jp.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_jp_long"}),
                    label_tbl_jpcrp_en.query("role == 'label'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en"}),
                    label_tbl_jpcrp_en.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en_long"}),
                ],
                how="left",
            )

        label_tbl_jppfs_jp = self.get_label_common_obj_jppfs_lab.export_label_tbl(
            label_to_taxonomi_dict=self.label_to_taxonomi_dict,
        )
        df_jppfs = (
            label_tbl_jppfs_jp.query("role == 'label'")
            .drop_duplicates(subset="key")
            .set_index("key")
            .rename(columns={"text": "label_jp"})
        )

        if not short_label_only:
            label_tbl_jppfs_en = (
                self.get_label_common_obj_jppfs_lab_en.export_label_tbl(
                    label_to_taxonomi_dict=self.label_to_taxonomi_dict,
                )
            )
            df_jppfs = df_jppfs.join(
                [
                    label_tbl_jppfs_jp.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_jp_long"}),
                    label_tbl_jppfs_en.query("role == 'label'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en"}),
                    label_tbl_jppfs_en.query("role == 'verboseLabel'")
                    .drop_duplicates(subset="key")
                    .set_index("key")[["text"]]
                    .rename(columns={"text": "label_en_long"}),
                ],
                how="left",
            )
        self.assign_common_label_df = pd.concat([df_jpcrp, df_jppfs]).drop_duplicates()
