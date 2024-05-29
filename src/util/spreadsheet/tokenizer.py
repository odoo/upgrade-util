import re

"""
This entire file is a direct translation of the original JavaScript code found in https://github.com/odoo/o-spreadsheet/blob/master/src/formulas/tokenizer.ts.
"""


class CellErrorType:
    NotAvailable = "#N/A"
    InvalidReference = "#REF"
    BadExpression = "#BAD_EXPR"
    CircularDependency = "#CYCLE"
    UnknownFunction = "#NAME?"
    DivisionByZero = "#DIV/0!"
    GenericError = "#ERROR"


DEFAULT_LOCALES = [
    {
        "name": "English (US)",
        "code": "en_US",
        "thousandsSeparator": ",",
        "decimalSeparator": ".",
        "dateFormat": "m/d/yyyy",
        "timeFormat": "hh:mm:ss a",
        "formulaArgSeparator": ",",
    }
]
DEFAULT_LOCALE = DEFAULT_LOCALES[0]

NEWLINE = "\n"


def get_formula_number_regex(decimal_separator):
    decimal_separator = re.escape(decimal_separator)
    return re.compile(r"^-?\d+(%s?\d*(e\d+)?)?|^-?%s\d+(?!\w|!)" % (decimal_separator, decimal_separator))


def escape_regexp(string):
    return re.escape(string)


full_row_xc = r"(\$?[A-Z]{1,3})?\$?[0-9]{1,7}\s*:\s*(\$?[A-Z]{1,3})?\$?[0-9]{1,7}\s*"
full_col_xc = r"\$?[A-Z]{1,3}(\$?[0-9]{1,7})?\s*:\s*\$?[A-Z]{1,3}(\$?[0-9]{1,7})?\s*"

cell_reference = re.compile(r"\$?([A-Z]{1,3})\$?([0-9]{1,7})", re.IGNORECASE)
range_reference = re.compile(
    r"^\s*('.+'!|[^']+!)?(%s|%s|%s)$" % (cell_reference.pattern, full_row_xc, full_col_xc), re.IGNORECASE
)

white_space_special_characters = [
    "\t",
    "\f",
    "\v",
    chr(int("00a0", 16)),
    chr(int("1680", 16)),
    chr(int("2000", 16)),
    chr(int("200a", 16)),
    chr(int("2028", 16)),
    chr(int("2029", 16)),
    chr(int("202f", 16)),
    chr(int("205f", 16)),
    chr(int("3000", 16)),
    chr(int("feff", 16)),
]
white_space_regexp = re.compile("|".join(map(re.escape, white_space_special_characters)) + r"|(\r\n|\r|\n)")


def replace_special_spaces(text):
    if not text:
        return ""
    if not white_space_regexp.search(text):
        return text
    return white_space_regexp.sub(lambda match: NEWLINE if match.group(1) else " ", text)


POSTFIX_UNARY_OPERATORS = ["%"]
OPERATORS = "+,-,*,/,:,=,<>,>=,>,<=,<,^,&".split(",") + POSTFIX_UNARY_OPERATORS


def tokenize(string, locale=DEFAULT_LOCALE):
    string = replace_special_spaces(string)
    result = []
    if string:
        chars = TokenizingChars(string)

        while not chars.is_over():
            token = (
                tokenize_space(chars)
                or tokenize_args_separator(chars, locale)
                or tokenize_parenthesis(chars)
                or tokenize_operator(chars)
                or tokenize_string(chars)
                or tokenize_debugger(chars)
                or tokenize_invalid_range(chars)
                or tokenize_number(chars, locale)
                or tokenize_symbol(chars)
            )

            if not token:
                token = ("UNKNOWN", chars.shift())

            result.append(token)

    return result


def tokenize_debugger(chars):
    if chars.current == "?":
        chars.shift()
        return "DEBUGGER", "?"
    return None


parenthesis = {"(": ("LEFT_PAREN", "("), ")": ("RIGHT_PAREN", ")")}


def tokenize_parenthesis(chars):
    value = chars.current
    if value in parenthesis:
        chars.shift()
        return parenthesis[value]
    return None


def tokenize_args_separator(chars, locale):
    if chars.current == locale["formulaArgSeparator"]:
        value = chars.shift()
        return "ARG_SEPARATOR", value
    return None


def tokenize_operator(chars):
    for op in OPERATORS:
        if chars.current_starts_with(op):
            chars.advance_by(len(op))
            return "OPERATOR", op
    return None


FIRST_POSSIBLE_NUMBER_CHARS = set("0123456789")


def tokenize_number(chars, locale):
    if chars.current not in FIRST_POSSIBLE_NUMBER_CHARS and chars.current != locale["decimalSeparator"]:
        return None
    match = re.match(get_formula_number_regex(locale["decimalSeparator"]), chars.remaining())
    if match:
        chars.advance_by(len(match.group(0)))
        return "NUMBER", match.group(0)
    return None


def tokenize_string(chars):
    if chars.current == '"':
        start_char = chars.shift()
        letters = start_char
        while chars.current and (chars.current != start_char or letters[-1] == "\\"):
            letters += chars.shift()
        if chars.current == '"':
            letters += chars.shift()
        return "STRING", letters
    return None


separator_regexp = re.compile(r"^[\w\.!\$]+")


def tokenize_symbol(chars):
    result = ""
    if chars.current == "'":
        last_char = chars.shift()
        result += last_char
        while chars.current:
            last_char = chars.shift()
            result += last_char
            if last_char == "'":
                if chars.current and chars.current == "'":
                    last_char = chars.shift()
                    result += last_char
                else:
                    break
        if last_char != "'":
            return "UNKNOWN", result
    match = separator_regexp.match(chars.remaining())
    if match:
        value = match.group(0)
        result += value
        chars.advance_by(len(value))
    if result:
        value = result
        is_reference = range_reference.match(value)
        if is_reference:
            return "REFERENCE", value
        return "SYMBOL", value
    return None


def tokenize_space(chars):
    length = 0
    while chars.current == NEWLINE:
        length += 1
        chars.shift()
    if length:
        return "SPACE", NEWLINE * length

    while chars.current == " ":
        length += 1
        chars.shift()

    if length:
        return "SPACE", " " * length
    return None


def tokenize_invalid_range(chars):
    if chars.current.startswith(CellErrorType.InvalidReference):
        chars.advance_by(len(CellErrorType.InvalidReference))
        return "INVALID_REFERENCE", CellErrorType.InvalidReference
    return None


class TokenizingChars:
    def __init__(self, text):
        self.text = text
        self.current_index = 0
        self.current = text[0]

    def shift(self):
        current = self.current
        self.current_index += 1
        self.current = self.text[self.current_index] if self.current_index < len(self.text) else None
        return current

    def advance_by(self, length):
        self.current_index += length
        self.current = self.text[self.current_index] if self.current_index < len(self.text) else None

    def is_over(self):
        return self.current_index >= len(self.text)

    def remaining(self):
        return self.text[self.current_index :]

    def current_starts_with(self, string):
        if self.current != string[0]:
            return False
        return all(self.text[self.current_index + j] == string[j] for j in range(1, len(string)))
