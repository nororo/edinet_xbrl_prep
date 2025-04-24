"""Tests for the utils module."""

import unittest
from edinet_xbrl_prep.utils import format_taxonomi


class TestFormatTaxonomi(unittest.TestCase):
    """Test cases for format_taxonomi function."""

    def test_format_taxonomi_standard_case(self):
        """Test format_taxonomi with standard input pattern."""
        # Setup
        input_str = "jpcrp030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxesPayableOpeCF"
        expected = "jpcrp030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxesPayableOpeCF"
        
        # Execute
        result = format_taxonomi(input_str)
        
        # Verify
        self.assertEqual(result, expected)
    
    def test_format_taxonomi_multiple_underscores(self):
        """Test format_taxonomi with multiple underscores in the input."""
        # Setup
        input_str = "jpcrp_030000-asr_E37207-000_IncreaseDecreaseInIncomeTaxes"
        expected = "jpcrp_030000-asr_E37207-000:IncreaseDecreaseInIncomeTaxes"
        
        # Execute
        result = format_taxonomi(input_str)
        
        # Verify
        self.assertEqual(result, expected)
    
    def test_format_taxonomi_single_underscore(self):
        """Test format_taxonomi with a single underscore in the input."""
        # Setup
        input_str = "prefix_value"
        expected = "prefix:value"
        
        # Execute
        result = format_taxonomi(input_str)
        
        # Verify
        self.assertEqual(result, expected)
    
    def test_format_taxonomi_empty_string(self):
        """Test format_taxonomi with an empty string."""
        # 空文字列の場合はValueErrorが発生するはず
        with self.assertRaises(ValueError) as context:
            format_taxonomi("")
        
        # エラーメッセージの確認
        self.assertIn("入力文字列が空", str(context.exception))
    
    def test_format_taxonomi_no_underscore(self):
        """Test format_taxonomi with no underscore in the input."""
        # アンダースコアがない場合はValueErrorが発生するはず
        with self.assertRaises(ValueError) as context:
            format_taxonomi("NoUnderscoreInThisString")
        
        # エラーメッセージの確認
        self.assertIn("アンダースコア", str(context.exception))


if __name__ == "__main__":
    unittest.main()
