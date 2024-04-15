from odoo.addons.base.maintenance.migrations.spreadsheet.tokenizer import tokenize
from odoo.addons.base.maintenance.migrations.testing import UnitTestCase


class SpreadsheetTokenizeTest(UnitTestCase):
    def test_simple_token(self):
        self.assertEqual(tokenize("1"), [("NUMBER", "1")])

    def test_number_with_decimal_token(self):
        self.assertEqual(
            tokenize("=1.5"),
            [("OPERATOR", "="), ("NUMBER", "1.5")],
        )

    def test_formula_token(self):
        self.assertEqual(
            tokenize("=1"),
            [("OPERATOR", "="), ("NUMBER", "1")],
        )

    def test_longer_operators(self):
        self.assertEqual(
            tokenize("= >= <= < <>"),
            [
                ("OPERATOR", "="),
                ("SPACE", " "),
                ("OPERATOR", ">="),
                ("SPACE", " "),
                ("OPERATOR", "<="),
                ("SPACE", " "),
                ("OPERATOR", "<"),
                ("SPACE", " "),
                ("OPERATOR", "<>"),
            ],
        )

    def test_concat_operator(self):
        self.assertEqual(tokenize("=&"), [("OPERATOR", "="), ("OPERATOR", "&")])

    def test_not_equal_operator(self):
        self.assertEqual(tokenize("=<>"), [("OPERATOR", "="), ("OPERATOR", "<>")])

    def test_can_tokenize_various_number_expressions(self):
        self.assertEqual(
            tokenize("1%"),
            [("NUMBER", "1"), ("OPERATOR", "%")],
        )
        self.assertEqual(tokenize("1 %"), [("NUMBER", "1"), ("SPACE", " "), ("OPERATOR", "%")])
        self.assertEqual(tokenize("1.1"), [("NUMBER", "1.1")])
        self.assertEqual(tokenize("1e3"), [("NUMBER", "1e3")])

    def test_debug_formula_token(self):
        self.assertEqual(
            tokenize("=?1"),
            [("OPERATOR", "="), ("DEBUGGER", "?"), ("NUMBER", "1")],
        )

    def test_REF_formula_token(self):
        tokens = tokenize("=#REF+1")
        self.assertEqual(
            tokens,
            [("OPERATOR", "="), ("UNKNOWN", "#"), ("SYMBOL", "REF"), ("OPERATOR", "+"), ("NUMBER", "1")],
        )

    def test_string(self):
        self.assertEqual(tokenize('"hello"'), [("STRING", '"hello"')])
        self.assertEqual(tokenize("'hello'"), [("SYMBOL", "'hello'")])
        self.assertEqual(tokenize("'hello"), [("UNKNOWN", "'hello")])
        self.assertEqual(tokenize('"he\\"l\\"lo"'), [("STRING", '"he\\"l\\"lo"')])
        self.assertEqual(tokenize("\"hel'l'o\""), [("STRING", "\"hel'l'o\"")])
        self.assertEqual(
            tokenize('"hello""test"'),
            [
                ("STRING", '"hello"'),
                ("STRING", '"test"'),
            ],
        )

    def test_function_missing_closing_parenthesis(self):
        tokens = tokenize("SUM(")
        self.assertEqual(tokens, [("SYMBOL", "SUM"), ("LEFT_PAREN", "(")])

    def test_function_token_with_point(self):
        self.assertEqual(tokenize("CEILING.MATH"), [("SYMBOL", "CEILING.MATH")])
        self.assertEqual(tokenize("ceiling.math"), [("SYMBOL", "ceiling.math")])
        self.assertEqual(
            tokenize("CEILING.MATH()"),
            [("SYMBOL", "CEILING.MATH"), ("LEFT_PAREN", "("), ("RIGHT_PAREN", ")")],
        )
        self.assertEqual(
            tokenize("ceiling.math()"),
            [("SYMBOL", "ceiling.math"), ("LEFT_PAREN", "("), ("RIGHT_PAREN", ")")],
        )

    def test_boolean(self):
        self.assertEqual(tokenize("true"), [("SYMBOL", "true")])
        self.assertEqual(tokenize("false"), [("SYMBOL", "false")])
        self.assertEqual(tokenize("TRUE"), [("SYMBOL", "TRUE")])
        self.assertEqual(tokenize("FALSE"), [("SYMBOL", "FALSE")])
        self.assertEqual(tokenize("TrUe"), [("SYMBOL", "TrUe")])
        self.assertEqual(tokenize("FalSe"), [("SYMBOL", "FalSe")])
        self.assertEqual(
            tokenize("=AND(true,false)"),
            [
                ("OPERATOR", "="),
                ("SYMBOL", "AND"),
                ("LEFT_PAREN", "("),
                ("SYMBOL", "true"),
                ("ARG_SEPARATOR", ","),
                ("SYMBOL", "false"),
                ("RIGHT_PAREN", ")"),
            ],
        )
        self.assertEqual(
            tokenize("=trueee"),
            [("OPERATOR", "="), ("SYMBOL", "trueee")],
        )

    def test_references(self):
        self.assertEqual(
            tokenize("=A1"),
            [("OPERATOR", "="), ("REFERENCE", "A1")],
        )
        self.assertEqual(
            tokenize("= A1 "),
            [
                ("OPERATOR", "="),
                ("SPACE", " "),
                ("REFERENCE", "A1"),
                ("SPACE", " "),
            ],
        )
        self.assertEqual(
            tokenize("=A1:A4"),
            [
                ("OPERATOR", "="),
                ("REFERENCE", "A1"),
                ("OPERATOR", ":"),
                ("REFERENCE", "A4"),
            ],
        )

    def test_fixed_references(self):
        self.assertEqual(tokenize("=$A$1"), [("OPERATOR", "="), ("REFERENCE", "$A$1")])
        self.assertEqual(tokenize("=A$1"), [("OPERATOR", "="), ("REFERENCE", "A$1")])
        self.assertEqual(tokenize("=$A1"), [("OPERATOR", "="), ("REFERENCE", "$A1")])
        self.assertEqual(tokenize("=Sheet1!$A1"), [("OPERATOR", "="), ("REFERENCE", "Sheet1!$A1")])
        self.assertEqual(tokenize("=Sheet1!A$1"), [("OPERATOR", "="), ("REFERENCE", "Sheet1!A$1")])
        self.assertEqual(tokenize("='Sheet1'!$A1"), [("OPERATOR", "="), ("REFERENCE", "'Sheet1'!$A1")])
        self.assertEqual(tokenize("='Sheet1'!A$1"), [("OPERATOR", "="), ("REFERENCE", "'Sheet1'!A$1")])

    def test_reference_and_sheets(self):
        self.assertEqual(
            tokenize("=Sheet1!A1"),
            [("OPERATOR", "="), ("REFERENCE", "Sheet1!A1")],
        )
        self.assertEqual(
            tokenize("=Sheet1!A1:A2"),
            [("OPERATOR", "="), ("REFERENCE", "Sheet1!A1"), ("OPERATOR", ":"), ("REFERENCE", "A2")],
        )
        self.assertEqual(
            tokenize("='Sheet1'!A1"),
            [("OPERATOR", "="), ("REFERENCE", "'Sheet1'!A1")],
        )
        self.assertEqual(
            tokenize("='Aryl Nibor Xela Nalim'!A1"),
            [("OPERATOR", "="), ("REFERENCE", "'Aryl Nibor Xela Nalim'!A1")],
        )
        self.assertEqual(
            tokenize("='a '' b'!A1"),
            [("OPERATOR", "="), ("REFERENCE", "'a '' b'!A1")],
        )

    def test_wrong_references(self):
        self.assertEqual(
            tokenize("='Sheet1!A1"),
            [("OPERATOR", "="), ("UNKNOWN", "'Sheet1!A1")],
        )
        self.assertEqual(
            tokenize("=!A1"),
            [("OPERATOR", "="), ("SYMBOL", "!A1")],
        )
        self.assertEqual(
            tokenize("=''!A1"),
            [("OPERATOR", "="), ("SYMBOL", "''!A1")],
        )
