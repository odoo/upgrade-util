from odoo.addons.base.maintenance.migrations.testing import UnitTestCase
from odoo.addons.base.maintenance.migrations.util.spreadsheet.parser import (
    BinaryOperation,
    FunctionCall,
    Literal,
    UnaryOperation,
    ast_to_string,
    parse,
)


class SpreadsheetParserTest(UnitTestCase):
    def test_can_parse_a_function_call_with_no_argument(self):
        self.assertEqual(parse("RAND()"), FunctionCall("RAND", []))

    def test_can_parse_a_function_call_with_one_argument(self):
        self.assertEqual(
            parse("SUM(1)"),
            FunctionCall("SUM", [Literal("NUMBER", "1")]),
        )

    def test_can_parse_a_function_call_with_function_argument(self):
        self.assertEqual(
            parse("SUM(UMINUS(1))"),
            FunctionCall("SUM", [FunctionCall("UMINUS", [Literal("NUMBER", "1")])]),
        )

    def test_can_parse_a_function_call_with_sub_expressions_as_argument(self):
        self.assertEqual(
            parse("IF(A1 > 0, 1, 2)"),
            FunctionCall(
                "IF",
                [
                    BinaryOperation(">", Literal("UNKNOWN", "A1"), Literal("NUMBER", "0")),
                    Literal("NUMBER", "1"),
                    Literal("NUMBER", "2"),
                ],
            ),
        )

    def test_add_a_unknown_token_for_empty_arguments(self):
        self.assertEqual(
            parse("SUM(1,)"),
            FunctionCall("SUM", [Literal("NUMBER", "1"), Literal("EMPTY", "")]),
        )

        self.assertEqual(
            parse("SUM(,1)"),
            FunctionCall("SUM", [Literal("EMPTY", ""), Literal("NUMBER", "1")]),
        )

        self.assertEqual(
            parse("SUM(,)"),
            FunctionCall("SUM", [Literal("EMPTY", ""), Literal("EMPTY", "")]),
        )

        self.assertEqual(
            parse("SUM(,,)"),
            FunctionCall("SUM", [Literal("EMPTY", ""), Literal("EMPTY", ""), Literal("EMPTY", "")]),
        )

        self.assertEqual(
            parse("SUM(,,,1)"),
            FunctionCall("SUM", [Literal("EMPTY", ""), Literal("EMPTY", ""), Literal("EMPTY", ""), Literal("NUMBER", "1")]),
        )

    def test_can_parse_unary_operations(self):
        self.assertEqual(
            parse("-1"),
            UnaryOperation("-", Literal("NUMBER", "1")),
        )
        self.assertEqual(
            parse("+1"),
            UnaryOperation("+", Literal("NUMBER", "1")),
        )

    def test_can_parse_numeric_values(self):
        self.assertEqual(parse("1"), Literal("NUMBER", "1"))
        self.assertEqual(parse("1.5"), Literal("NUMBER", "1.5"))
        self.assertEqual(parse("1."), Literal("NUMBER", "1."))
        self.assertEqual(parse(".5"), Literal("NUMBER", ".5"))

    def test_can_parse_string_values(self):
        self.assertEqual(parse('"Hello"'), Literal("STRING", "Hello"))

    def test_can_parse_number_expressed_as_percent(self):
        self.assertEqual(parse("1%"), Literal("NUMBER", "1%"))
        self.assertEqual(parse("100%"), Literal("NUMBER", "100%"))
        self.assertEqual(parse("50.0%"), Literal("NUMBER", "50.0%"))

    def test_can_parse_binary_operations(self):
        self.assertEqual(
            parse("2-3"),
            BinaryOperation("-", Literal("NUMBER", "2"), Literal("NUMBER", "3")),
        )

    def test_can_parse_concat_operator(self):
        self.assertEqual(
            parse("A1&A2"),
            BinaryOperation("&", Literal("UNKNOWN", "A1"), Literal("UNKNOWN", "A2")),
        )

    def test_AND(self):
        self.assertEqual(
            parse("=AND(true, false)"),
            FunctionCall("AND", [Literal("BOOLEAN", "true"), Literal("BOOLEAN", "false")]),
        )
        self.assertEqual(
            parse("=AND(0, tRuE)"),
            FunctionCall("AND", [Literal("NUMBER", "0"), Literal("BOOLEAN", "tRuE")]),
        )

    def test_convert_string(self):
        self.assertEqual(ast_to_string(parse('"hello"')), '"hello"')

    def test_convert_debugger(self):
        self.assertEqual(ast_to_string(parse("?5+2")), "5+2")

    def test_convert_boolean(self):
        self.assertEqual(ast_to_string(parse("TRUE")), "TRUE")
        self.assertEqual(ast_to_string(parse("FALSE")), "FALSE")

    def test_convert_unary_operator(self):
        self.assertEqual(ast_to_string(parse("-45")), "-45")
        self.assertEqual(ast_to_string(parse("+45")), "+45")
        self.assertEqual(ast_to_string(parse("-(4+5)")), "-(4+5)")
        self.assertEqual(ast_to_string(parse("-4+5")), "-4+5")
        self.assertEqual(ast_to_string(parse("-SUM(1)")), "-SUM(1)")
        self.assertEqual(ast_to_string(parse("-(1+2)/5")), "-(1+2)/5")
        self.assertEqual(ast_to_string(parse("1*-(1+2)")), "1*-(1+2)")

    def test_convert_binary_operator(self):
        self.assertEqual(ast_to_string(parse("89-45")), "89-45")
        self.assertEqual(ast_to_string(parse("1+2+5")), "1+2+5")
        self.assertEqual(ast_to_string(parse("(1+2)/5")), "(1+2)/5")
        self.assertEqual(ast_to_string(parse("5/(1+2)")), "5/(1+2)")
        self.assertEqual(ast_to_string(parse("2/(1*2)")), "2/(1*2)")
        self.assertEqual(ast_to_string(parse("1-2+3")), "1-2+3")
        self.assertEqual(ast_to_string(parse("1-(2+3)")), "1-(2+3)")
        self.assertEqual(ast_to_string(parse("(1+2)-3")), "1+2-3")
        self.assertEqual(ast_to_string(parse("(1<5)+5")), "(1<5)+5")
        self.assertEqual(ast_to_string(parse("1*(4*2+3)")), "1*(4*2+3)")
        self.assertEqual(ast_to_string(parse("1*(4+2*3)")), "1*(4+2*3)")
        self.assertEqual(ast_to_string(parse("1*(4*2+3*9)")), "1*(4*2+3*9)")
        self.assertEqual(ast_to_string(parse("1*(4-(2+3))")), "1*(4-(2+3))")
        self.assertEqual(ast_to_string(parse("1/(2*(2+3))")), "1/(2*(2+3))")
        self.assertEqual(ast_to_string(parse("1/((2+3)*2)")), "1/((2+3)*2)")
        self.assertEqual(ast_to_string(parse("2<(1<1)")), "2<(1<1)")
        self.assertEqual(ast_to_string(parse("2<=(1<1)")), "2<=(1<1)")
        self.assertEqual(ast_to_string(parse("2>(1<1)")), "2>(1<1)")
        self.assertEqual(ast_to_string(parse("2>=(1<1)")), "2>=(1<1)")
        self.assertEqual(ast_to_string(parse("TRUE=1=1")), "TRUE=1=1")
        self.assertEqual(ast_to_string(parse("TRUE=(1=1)")), "TRUE=(1=1)")

    def test_convert_function(self):
        self.assertEqual(ast_to_string(parse("SUM(5,9,8)")), "SUM(5,9,8)")
        self.assertEqual(ast_to_string(parse("-SUM(5,9,SUM(5,9,8))")), "-SUM(5,9,SUM(5,9,8))")

    def test_convert_references(self):
        self.assertEqual(ast_to_string(parse("A10")), "A10")
        self.assertEqual(ast_to_string(parse("Sheet1!A10")), "Sheet1!A10")
        self.assertEqual(ast_to_string(parse("'Sheet 1'!A10")), "'Sheet 1'!A10")
        self.assertEqual(ast_to_string(parse("'Sheet 1'!A10:A11")), "'Sheet 1'!A10:A11")
        self.assertEqual(ast_to_string(parse("SUM(A1,A2)")), "SUM(A1,A2)")

    def test_convert_strings(self):
        self.assertEqual(ast_to_string(parse('"R"')), '"R"')
        self.assertEqual(ast_to_string(parse('CONCAT("R", "EM")')), 'CONCAT("R","EM")')

    def test_convert_numbers(self):
        self.assertEqual(ast_to_string(parse("5")), "5")
        self.assertEqual(ast_to_string(parse("5+4")), "5+4")
        self.assertEqual(ast_to_string(parse("+5")), "+5")
        self.assertEqual(ast_to_string(parse("1%")), "1%")
        self.assertEqual(ast_to_string(parse("1.5")), "1.5")
        self.assertEqual(ast_to_string(parse("1.")), "1.")
        self.assertEqual(ast_to_string(parse(".5")), ".5")
