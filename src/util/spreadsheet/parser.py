from dataclasses import dataclass
from typing import Callable, ClassVar, Iterable, List, Union

from .tokenizer import (
    POSTFIX_UNARY_OPERATORS,
    tokenize,
)

OP_PRIORITY = {
    "^": 30,
    "%": 30,
    "*": 20,
    "/": 20,
    "+": 15,
    "-": 15,
    "&": 13,
    ">": 10,
    "<>": 10,
    ">=": 10,
    "<": 10,
    "<=": 10,
    "=": 10,
}

UNARY_OPERATORS_PREFIX = ["-", "+"]

AST = Union["Literal", "BinaryOperation", "UnaryOperation", "FunctionCall"]


@dataclass
class Literal:
    type: str
    value: str


@dataclass
class BinaryOperation:
    type: ClassVar[str] = "BIN_OPERATION"
    value: str
    left: AST
    right: AST


@dataclass
class UnaryOperation:
    type: ClassVar[str] = "UNARY_OPERATION"
    value: str
    operand: AST
    postfix: bool = False


@dataclass
class FunctionCall(Literal):
    type: ClassVar[str] = "FUNCALL"
    value: str
    args: List[AST]


def parse(formula):
    """Parse a spreadsheet formula and return an AST"""
    tokens = tokenize(formula)
    tokens = [token for token in tokens if token[0] != "DEBUGGER"]
    if tokens[0][1] == "=":
        tokens = tokens[1:]
    return parse_expression(tokens)


def parse_expression(tokens, binding_power=0) -> AST:
    if not tokens:
        raise ValueError("Unexpected end of formula")
    left = parse_operand(tokens)
    # as long as we have operators with higher priority than the parent one,
    # continue parsing the expression since we are in a child sub-expression
    while tokens and tokens[0][0] == "OPERATOR" and OP_PRIORITY[tokens[0][1]] > binding_power:
        operator = tokens.pop(0)[1]
        if operator in POSTFIX_UNARY_OPERATORS:
            left = UnaryOperation(operator, left, postfix=True)
        else:
            right = parse_expression(tokens, OP_PRIORITY[operator])
            left = BinaryOperation(operator, left, right)
    return left


def parse_function_args(tokens) -> Iterable[AST]:
    consume_or_raise(tokens, "LEFT_PAREN", error_msg="Missing opening parenthesis")
    if not tokens:
        raise ValueError("Unexpected end of formula")
    next_token = tokens[0]
    if next_token[0] == "RIGHT_PAREN":
        consume_or_raise(tokens, "RIGHT_PAREN")
        yield from []
        return
    yield parse_single_arg(tokens)
    while tokens and tokens[0][0] != "RIGHT_PAREN":
        consume_or_raise(tokens, "ARG_SEPARATOR", error_msg="Wrong function call")
        yield parse_single_arg(tokens)
    consume_or_raise(tokens, "RIGHT_PAREN", error_msg="Missing closing parenthesis")


def parse_single_arg(tokens) -> AST:
    next_token = tokens[0]
    if next_token[0] in {"ARG_SEPARATOR", "RIGHT_PAREN"}:
        # arg is empty: "sum(1,,2)" "sum(,1)" "sum(1,)"
        return Literal("EMPTY", "")
    return parse_expression(tokens)


def parse_operand(tokens: list) -> AST:
    token = tokens.pop(0)
    if token[0] == "LEFT_PAREN":
        expr = parse_expression(tokens)
        consume_or_raise(tokens, "RIGHT_PAREN", error_msg="Missing closing parenthesis")
        return expr
    elif token[0] == "STRING":
        return Literal(token[0], token[1].strip('"'))
    elif token[0] in ["NUMBER", "BOOLEAN", "UNKNOWN"]:
        return Literal(token[0], token[1])
    elif token[0] == "OPERATOR" and token[1] in UNARY_OPERATORS_PREFIX:
        operator = token[1]
        return UnaryOperation(operator, parse_expression(tokens, OP_PRIORITY[operator]))
    elif token[0] == "SYMBOL":
        # breakpoint()
        args = list(parse_function_args(tokens))
        return FunctionCall(token[1], list(args))
    raise ValueError(f"Unexpected token: {token}")


def consume_or_raise(tokens, token_type, error_msg="Unexpected token"):
    if not tokens or tokens.pop(0)[0] != token_type:
        raise ValueError(error_msg)


def ast_to_string(ast: AST) -> str:
    """Convert an AST to the corresponding string."""
    if ast.type == "BIN_OPERATION":
        operator = ast.value
        left = left_to_string(ast.left, operator)
        right = right_to_string(ast.right, operator)
        return f"{left}{operator}{right}"
    elif ast.type == "UNARY_OPERATION":
        operator = ast.value
        if ast.postfix:
            return f"{left_to_string(ast.operand, operator)}{operator}"
        else:
            return f"{operator}{right_to_string(ast.operand, operator)}"
    elif ast.type == "FUNCALL":
        args = (ast_to_string(arg) for arg in ast.args)
        return f"{ast.value}({','.join(args)})"
    elif ast.type == "STRING":
        return f'"{ast.value}"'
    elif ast.type in {"NUMBER", "BOOLEAN", "EMPTY", "UNKNOWN"}:
        return ast.value
    raise ValueError("Unexpected node type: " + ast.type)


def left_to_string(left_expr: AST, parent_operator: str) -> str:
    """Convert the left operand of a binary operation to the corresponding string
    and enclose the result inside parenthesis if necessary."""
    if left_expr.type == "BIN_OPERATION" and OP_PRIORITY[left_expr.value] < OP_PRIORITY[parent_operator]:
        return f"({ast_to_string(left_expr)})"
    return ast_to_string(left_expr)


ASSOCIATIVE_OPERATORS = {"*", "+", "&"}


def right_to_string(right_expr: AST, parent_operator: str) -> str:
    """Convert the right operand of a binary operation to the corresponding string
    and enclose the result inside parenthesis if necessary."""
    if right_expr.type != "BIN_OPERATION":
        return ast_to_string(right_expr)
    elif (OP_PRIORITY[right_expr.value] < OP_PRIORITY[parent_operator]) or (
        OP_PRIORITY[right_expr.value] == OP_PRIORITY[parent_operator] and parent_operator not in ASSOCIATIVE_OPERATORS
    ):
        return f"({ast_to_string(right_expr)})"
    return ast_to_string(right_expr)


def transform_ast_nodes(ast: AST, node_type: str, node_transformer: Callable[[AST], AST]) -> AST:
    """Transform the nodes of an AST using the provided node_transformer function."""
    if ast.type == node_type:
        ast = node_transformer(ast)
    if ast.type == "BIN_OPERATION":
        return BinaryOperation(
            ast.value,
            transform_ast_nodes(ast.left, node_type, node_transformer),
            transform_ast_nodes(ast.right, node_type, node_transformer),
        )
    elif ast.type == "UNARY_OPERATION":
        return UnaryOperation(
            ast.value,
            transform_ast_nodes(ast.operand, node_type, node_transformer),
            ast.postfix,
        )
    elif ast.type == "FUNCALL":
        return FunctionCall(
            ast.value,
            list(transform_ast_nodes(arg, node_type, node_transformer) for arg in ast.args),
        )
    else:
        return ast
