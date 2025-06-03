import unittest
from pathlib import Path

from edinet_xbrl_prep.account_list_common import account_list_common_cor

TESTDIR = Path(__file__) / "test_data" / "account_list"


class TestAccountListCommon(unittest.TestCase):
    """Test account_list_common_

    assertion:
    - label_tbl_jpcrp_jp is not empty
    - label_tbl_jpcrp_en is not empty
    - label_jp and label_jp_long are different
    - label_jp and label_en are different
    """

    def setUp(self):
        year_str = "2024"
        self.account_list_common_obj_dict = {
            year_str: account_list_common_cor(
                data_path=TESTDIR,
                account_list_year=year_str,
            ),
        }

    def tearDown(self):
        pass

    def test_contains_ex(self):
        account_list_common_obj = self.account_list_common_obj_dict["2024"]

        input_str = "CommissionFee"
        expected_int = 6
        rst = len(
            account_list_common_obj.assign_common_label_df.query(
                "index.str.contains(@input_str)",
            ),
        )
        assert rst >= expected_int, "lack label converted key for {input_str}"

    def test_account_list_common(self):
        input_str = "jpcrp_cor:CabinetOfficeOrdinanceOnDisclosureOfCorporateInformationEtcFormNo3AnnualSecuritiesReportHeading"
        account_list_common_obj = self.account_list_common_obj_dict["2024"]
        label_tbl_jpcrp_jp = (
            account_list_common_obj.get_label_common_obj_jpcrp_lab.export_label_tbl(
                label_to_taxonomi_dict=account_list_common_obj.label_to_taxonomi_dict,
            )
        )

        label_tbl_jpcrp_en = (
            account_list_common_obj.get_label_common_obj_jpcrp_lab_en.export_label_tbl(
                label_to_taxonomi_dict=account_list_common_obj.label_to_taxonomi_dict,
            )
        )
        df_jpcrp = (
            label_tbl_jpcrp_jp.query("role == 'label'")
            .drop_duplicates(subset="key")
            .set_index("key")
            .rename(columns={"text": "label_jp"})
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
        assert len(df_jpcrp) > 0
        assert (
            df_jpcrp.loc[
                input_str,
                "label_jp",
            ]
            != df_jpcrp.loc[
                input_str,
                "label_jp_long",
            ]
        )
        assert (
            df_jpcrp.loc[
                input_str,
                "label_jp",
            ]
            != df_jpcrp.loc[
                input_str,
                "label_en",
            ]
        )
