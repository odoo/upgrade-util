"""
This file contains tools for making views compatible with Odoo 17.

Two main changes are performed here:
1. Convert `attrs` attributes of view elements from domains into Python expressions.
2. Remove `states` attribute, merge its logic into `invisible` attribute.

The main entry point is `fix_attrs(cr, model, arch, comb_arch)` -- see docstring for
details about its parameters. One important consideration when converting views is that
we must convert views in the right order to ensure the arch is built correctly. Thus we
should convert root views first, then continue with children view, respecting also the
sequence of the views.

The script can be imported directly or via `util.import_script`. The model of the view
should be fully loaded before attempting to convert it -- thus an end- script is the
best place to use this tool. See adapt_view function in this file for an example usage
of the utlities in this script.

Example usage from another script:
```
from odoo.upgrade import util

script = util.import_script("base/17.0.1.3/attr_domains2expr.py")

def migrate(cr, version):
    # script.adapt_view(...)
    # script.fix_attrs(...)
    pass
```
"""

import ast
import logging
import re
import uuid

from lxml import etree

from odoo.osv.expression import DOMAIN_OPERATORS, normalize_domain
from odoo.tools.safe_eval import safe_eval

from odoo.upgrade import util


def adapt_view(cr, view_xmlid):
    """
    Adapt one view.

    Example usage of the utilities in this file.

    We use `util.edit_view` because it handles the propagation of the changes to all
    languages while updating the whole arch. Alternatively you could just update specific
    elements.

    Note if a view needs to be adapted for a specific languages use the `lang` parameter
    in the context:
    ```
    IrUiView = util.env(cr)["ir.ui.view"].with_context(lang=lang)
    ```
    """
    vid = util.ref(view_xmlid)
    IrUiView = util.env(cr)["ir.ui.view"]
    view = IrUiView.browse(vid)

    # disable view to avoid it being applied to the parent arch
    view.active = False
    # get combined arch of the parent view
    comb_arch = view.inherit_id._get_combined_arch()

    # update current view arch
    new_arch = etree.fromstring(view.arch_db)
    fix_attrs(cr, view.model, new_arch, comb_arch)

    # `new_arch` is now already transformed, we can now copy its value
    # note: we re-activate the view
    with util.edit_view(cr, view_id=vid, active=True) as arch:
        arch.clear()
        arch.attrib.update(new_arch.attrib)
        arch.text = new_arch.text
        arch.extend(new_arch)


_logger = logging.getLogger(__name__)

MODS = ["invisible", "readonly", "required", "column_invisible"]
DEFAULT_CONTEXT_REPLACE = {"active_id": "id"}
LIST_HEADER_CONTEXT_REPLACE = {"active_id": "context.get('active_id')"}


class InvalidDomainError(Exception):
    pass


class Ast2StrVisitor(ast._Unparser):
    """Extend standard unparser to allow specific names to be replaced."""

    def __init__(self, replace_names=None):
        self._replace_names = replace_names if replace_names else DEFAULT_CONTEXT_REPLACE
        super().__init__()

    def visit_Name(self, node):
        return self.write(self._replace_names.get(node.id, node.id))


def mod2bool_str(s):
    """
    Convert yes/no/true/false/on/off into True/False strings.

    Otherwise returns the input unchanged.
    The checked values would raise an error instead if used in a Python expression.
    Note that 0 and 1 are left unchanged since they have the same True/False meaning in Python.
    """
    ss = s.lower()
    if ss in "yes true on".split():
        return "True"
    if ss in "no false off".split():
        return "False"
    return s


def _clean_bool(s):
    """Minimal simplification of trivial boolean expressions."""
    return {
        "(1)": "1",
        "(0)": "0",
        "(True)": "True",
        "(False)": "False",
        "not (True)": "False",
        "not (False)": "True",
    }.get(s, s)


def target_elem_and_view_type(elem, comb_arch):
    """
    Find the target of an element.

    If there is no `comb_arch` or the element doesn't look like
    targeting anything (no position attributes) assume the input `elem` is the target and return it.
    Along with the target we also return the view type of the elem, plus the field path from the
    arch root.
    """

    def find_target(elem):
        # as in standard: github.com/odoo/odoo/blob/4fec6300/odoo/tools/template_inheritance.py#L73-L94
        if comb_arch is not None and elem.get("position"):
            if elem.tag == "xpath":
                it = iter(comb_arch.xpath(elem.get("expr")))
            elif elem.tag == "field":
                it = (x for x in comb_arch.iter("field") if x.get("name") == elem.get("name"))
            else:
                it = (
                    x
                    for x in comb_arch.iter("field")
                    if all(x.get(k) == elem.get(k) for k in elem.attrib if k != "position")
                )
            return next(it, elem)
        return elem

    field_path = []
    view_type = None
    telem = find_target(elem)
    pelem = telem.getparent()
    while pelem is not None:
        # the parent may be a targeting element (xpath or field tag with position attribute)
        # thus we need to ensure we got the parent's target
        pelem_target_position = pelem.get("position")
        pelem = find_target(pelem)
        if view_type is None and pelem.tag in (
            "kanban",
            "tree",
            "form",
            "calendar",
            "setting",
            "search",
            "templates",
            "groupby",
        ):
            view_type = pelem.tag
        if pelem.tag == "field" and (not pelem_target_position or pelem_target_position == "inside"):
            # if element is a normal <field/> or a targeting element with position="inside"
            field_path.append(pelem.get("name"))
        pelem = pelem.getparent()
    field_path.reverse()
    return telem, view_type, field_path


def is_simple_pred(expr):
    if expr in ("0", "1", "True", "False"):
        return True
    if re.match(r"""context\.get\((['"])\w+\1\)""", expr):
        return True
    return False


def fix_elem(cr, model, elem, comb_arch):
    success = True
    telem, inner_view_type, field_path = target_elem_and_view_type(elem, comb_arch)

    if elem.get("position") != "replace":
        telem = None  # do not take default attributes from the target element

    # Build the dict of attrs attributes:
    # 1. Take the values from the target element if any
    # 2. If current element has attrs, override the values.
    #    All keys in target not in current element are overriden as empty value.
    attrs = {}
    if telem is not None and "attrs" in telem.attrib:
        ast_attrs = ast_parse(telem.get("attrs"))
        if isinstance(ast_attrs, ast.Dict):
            attrs = {k.value: v for k, v in zip(ast_attrs.keys, ast_attrs.values)}
        else:
            _logger.log(
                util.NEARLYWARN if util.on_CI() else logging.ERROR,
                "Removing invalid `attrs` value %r from\n%s",
                telem.get("attrs"),
                etree.tostring(telem).decode(),
            )

    if elem.get("attrs"):
        attrs_val = elem.get("attrs")
        ast_attrs = ast_parse(attrs_val)
        if isinstance(ast_attrs, ast.Dict):
            elem_attrs = {k.value: v for k, v in zip(ast_attrs.keys, ast_attrs.values)}
            attrs.update(elem_attrs)
            for k in attrs:
                if k not in elem_attrs:
                    attrs[k] = ast.Constant("")  # clear previous values
        else:
            _logger.log(
                util.NEARLYWARN if util.on_CI() else logging.ERROR,
                "Removing invalid `attrs` value %r from\n%s",
                attrs_val,
                etree.tostring(elem).decode(),
            )
    elem.attrib.pop("attrs", "")

    for mod in MODS:
        if mod not in elem.attrib and mod not in attrs:
            continue
        if inner_view_type == "kanban" and elem.tag == "field":
            # in kanban view, field outside <templates> should not have modifiers
            elem.attrib.pop(mod, None)
            continue
        # if mod is not in the blend of attrs from current element and target, then we don't
        # need to take the default value from target element since we can assume an override
        default_val = telem.get(mod, "") if telem is not None and mod in attrs else ""
        orig_mod = mod2bool_str(elem.get(mod, default_val).strip())
        try:
            attr_mod = (
                mod2bool_str(_clean_bool(convert_attrs_val(cr, model, field_path, attrs.get(mod))))
                if mod in attrs
                else ""
            )
        except InvalidDomainError as e:
            domain = e.args[0]
            _logger.error("Invalid domain `%s`, saved as data-upgrade-invalid-domain attribute", domain)  # noqa: TRY400
            hex_hash = uuid.uuid4().hex[:6]
            elem.attrib[f"data-upgrade-invalid-domain-{mod}-{hex_hash}"] = domain
            attr_mod = ""
            success = False
        # in list view we can switch the inline invisible into column_invisible
        # in case only the attrs invisible is present we can also use column_invisible
        if (
            mod == "invisible"
            and inner_view_type == "tree"
            and "column_invisible" not in elem.attrib
            and "column_invisible" not in attrs
        ):
            if is_simple_pred(orig_mod):
                elem.attrib.pop("invisible")
                elem.set("column_invisible", orig_mod)
                orig_mod = ""
            elif not orig_mod and is_simple_pred(attr_mod):
                elem.set("column_invisible", attr_mod)
                continue  # we know orig_mode is empty!

        # combine attributes
        if orig_mod and attr_mod:
            # short circuits for final_mod = (orig_mod or attr_mod)
            if orig_mod in ("True", "1") or attr_mod in ("True", "1"):
                final_mod = orig_mod
            elif orig_mod in ("False", "0"):
                final_mod = attr_mod
            elif attr_mod == ("False", "0"):
                final_mod = orig_mod
            else:
                final_mod = f"({orig_mod}) or ({attr_mod})"
        else:
            final_mod = orig_mod or attr_mod

        # set attribute if anything to set, or force empty if mod was present
        if final_mod or mod in attrs:
            elem.set(mod, final_mod)

    # special case to merge into invisible
    if "states" in elem.attrib:
        states = elem.attrib.pop("states")
        expr = "state not in [{}]".format(",".join(repr(x.strip()) for x in states.split(",")))
        invisible = elem.get("invisible")
        if invisible:
            elem.set("invisible", f"({invisible}) or ({expr})")
        else:
            elem.set("invisible", expr)

    for mod in MODS:
        attrs.pop(mod, None)
    # keys in attrs should be only one of MODS list, we inline here any "extra" value with a warning
    if attrs:
        extra = [key for key in attrs if key not in elem.attrib]
        _logger.log(
            util.NEARLYWARN if util.on_CI() else logging.WARN,
            "Extra values %s in `attrs` attribute will be inlined for element\n%s",
            extra,
            etree.tostring(elem).decode(),
        )
        extra_invalid = [key for key in attrs if key in elem.attrib]
        if extra_invalid:
            _logger.log(
                util.NEARLYWARN if util.on_CI() else logging.ERROR,
                "Attributes %s in `attrs` cannot be inlined because the inline attributes already exists",
                extra_invalid,
            )
        for key in extra:
            value = ast.unparse(attrs[key])
            _logger.info("Inlined %s=%r", key, value)
            elem.set(key, value)

    return success


def ast_parse(val):
    try:
        return ast.parse(val.strip(), mode="eval").body
    except SyntaxError:
        _logger.exception("Error for invalid code:\n%s", val)
        raise


def fix_attrs(cr, model, arch, comb_arch):
    """
    Update `arch` etree transforming all attrs elements from domains to Python expressions.

    `model`: the model name of the view
    `arch`: etree instance of the view's arch
    `comb_arch`: combined arch of the parent of current view, ignored if `None`. Used to merge
    parent attributes into current `arch` when necessary.

    Returns True on success. Transforms `arch` in-place.
    For an example usage refer to this file's docstring and `adapt_view`
    """
    success = True
    for elem in arch.xpath(
        "//attribute[@name='invisible' or @name='required' or @name='readonly' or @name='column_invisible']"
    ):
        if "value" in elem.attrib:
            elem.set("value", mod2bool_str(elem.get("value").strip()))
        elif elem.text:
            elem.text = mod2bool_str(elem.text.strip())

    # inline all attrs combined with already inline values
    for elem in arch.xpath("//*[@attrs or @states or @invisible or @required or @readonly or @column_invisible]"):
        success &= fix_elem(cr, model, elem, comb_arch)

    # remove context elements
    for elem in arch.xpath("//tree/header/*[contains(@context, 'active_id')]"):
        elem.set(
            "context",
            Ast2StrVisitor(LIST_HEADER_CONTEXT_REPLACE).visit(ast_parse(elem.get("context"))),
        )
    for elem in arch.xpath("//*[contains(@context, 'active_id')]"):
        elem.set("context", Ast2StrVisitor().visit(ast_parse(elem.get("context"))))

    # replace <attribute name=attrs> elements with individual <attribute name=mod>
    # use a fake field element to reuse the logic for Python expression conversion
    # <field name=... position=attributes>
    #   <attribute name=attrs>{invisible: xxx}</attribute>`
    #   <attribute name=invisible>yyy</attribute>`
    #   <attribute name=readonly value=zzz></attribute>`
    # </field>
    # becomes the fake element
    # <field name=... position=replace attrs={invisible: xxx} invisible=yyy readonly=zzz/>`
    for parent in arch.xpath("//*[@position='attributes']"):
        attrs_data = {}  # save the attributes from the children
        for elem in parent.findall("./attribute"):
            name = elem.get("name")
            if name in ["attrs", "states", *MODS]:
                attrs_data[name] = elem.get("value", elem.text or "").strip()
                parent.remove(elem)
            if name == "attrs" and not attrs_data["attrs"]:
                attrs_data["attrs"] = "{}"
        # keep track of extra keys in `attrs` if any
        extra_mods = [k.value for k in ast_parse(attrs_data.get("attrs", "{}")).keys if k.value not in MODS]
        fake_elem = etree.Element(parent.tag, {**parent.attrib, **attrs_data}, position="replace")
        success &= fix_elem(cr, model, fake_elem, comb_arch)
        for mod in MODS + extra_mods:
            if mod not in fake_elem.attrib:
                continue
            new_elem = etree.Element("attribute", name=mod)
            new_elem.text = fake_elem.get(mod)
            parent.append(new_elem)

    return success


def check_true_false(lv, ov, rv_ast):
    """
    Return True/False if the leaf (lp, op, rp) is something that can be considered as a True/False leaf.

    Otherwise returns None.
    """
    ov = {"=": "==", "<>": "!="}.get(ov, ov)
    if ov not in ["==", "!="]:
        return None
    # Note: from JS implementation (None,=,xxx) is always False, same for (True/False,=,xxx)
    #       conversely if op is `!=` then this is considered True ¯\_(ツ)_/¯
    if isinstance(lv, bool) or lv is None:
        return ov == "!="
    if isinstance(lv, (int, float)) and isinstance(rv_ast, ast.Constant) and isinstance(rv_ast.value, (int, float)):
        return safe_eval(f"{lv} {ov} {rv_ast.value}")
    return None


def ast_term2domain_term(term):
    if isinstance(term, ast.Constant) and term.value in DOMAIN_OPERATORS:
        return term.value
    if isinstance(term, (ast.Tuple, ast.List)):
        try:
            left, op, right = term.elts
        except Exception:
            _logger.error("Invalid domain leaf %s", ast.unparse(term))  # noqa: TRY400
            raise SyntaxError() from None
        else:
            return (left.value, op.value, right)
    _logger.error("Domain terms must be a domain operator or a three-elements tuple, got %s", ast.unparse(term))
    raise SyntaxError() from None


def convert_attrs_val(cr, model, field_path, val):
    """
    Convert an `attrs` value into a python formula.

    We need to use the AST representation because
    values representing domains could be:
    * an if, or boolean, expression returning alternative domains
    * a string constant with the domain
    * a list representing the domain directly
    """
    ast2str = Ast2StrVisitor().visit

    if isinstance(val, ast.IfExp):
        return "({} if {} else {})".format(
            convert_attrs_val(cr, model, field_path, val.body),
            ast2str(val.test),
            convert_attrs_val(cr, model, field_path, val.orelse),
        )
    if isinstance(val, ast.BoolOp):
        return "({})".format(
            (" and " if type(val.op) == ast.And else " or ").join(
                convert_attrs_val(cr, model, field_path, v) for v in val.values
            )
        )

    if isinstance(val, ast.Constant):  # {'readonly': '0'} or {'invisible': 'name'}
        val = str(val.value).strip()  # we process the right side as a string
        # a string should be interpreted as a field name unless it is a domain!!
        if val and val[0] == "[" and val[-1] == "]":
            val = ast_parse(val)
            return convert_attrs_val(cr, model, field_path, val)
        return mod2bool_str(val)

    if isinstance(val, ast.List):  # val is a domain
        orig_ast = val
        val = val.elts
        if not val:
            return "True"  # all records match the empty domain
        # make an ast domain look like a domain, to be able to use normalize_domain
        try:
            val = [ast_term2domain_term(term) for term in val]
            norm_domain = normalize_domain(val)
        except Exception:
            raise InvalidDomainError(ast.unparse(orig_ast)) from None
        # convert domain into python expression
        stack = []
        for item in reversed(norm_domain):
            if item == "!":
                top = stack.pop()
                stack.append(f"(not {top})")
            elif item in ("&", "|"):
                right = stack.pop()
                left = stack.pop()
                op = {"&": "and", "|": "or"}[item]
                stack.append(f"({left} {op} {right})")
            else:
                stack.append(convert_domain_leaf(cr, model, field_path, item))
        assert len(stack) == 1
        res = stack.pop()
        assert res[0] == "(" and res[-1] == ")", res
        return res[1:-1]

    return ast2str(val)


def target_field_type(cr, model, path):
    ttype = None
    for fname in path:
        cr.execute(
            """
            SELECT relation, ttype
              FROM ir_model_fields
             WHERE model = %s
               AND name = %s
             ORDER BY id
             LIMIT 1
            """,
            [model, fname],
        )
        model, ttype = cr.fetchone() if cr.rowcount else (None, None)
        if model is None:
            break
    return ttype


def convert_domain_leaf(cr, model, field_path, leaf):
    """
    Convert a domain leaf (tuple) into a python expression.

    It always return the expression surrounded by parenthesis such that it's safe to use it as a sub-expression.
    """
    if isinstance(leaf, bool):
        # JS allows almost everything in a domain, boolean fields have a clear meaning and they are
        # interpreted in JS side as their boolean value, we do the same here.
        return f"({leaf})"
    left, op, right_ast = leaf
    tf = check_true_false(left, op, right_ast)
    if tf is not None:
        return f"({tf})"

    # see warnings from osv.expression.normalize_leaf
    # https://github.com/odoo/odoo/blob/7ff1dac42fe24d1070c569f99ae7a67fe66eda2b/odoo/osv/expression.py#L353-L358
    if op in ("in", "not in") and isinstance(right_ast, ast.Constant) and isinstance(right_ast.value, bool):
        op = "=" if op == "in" else "!="
    elif op in ("=", "!=") and isinstance(right_ast, (ast.List, ast.Tuple)):
        op = "in" if op == "=" else "not in"

    right = Ast2StrVisitor().visit(right_ast)
    if op == "=?":
        return f"({right} is False or {right} is None or ({left} == {right}))"
    if op in ("=", "=="):
        return f"({left} == {right})"
    if op in ("!=", "<>"):
        return f"({left} != {right})"
    if op in ("<", "<=", ">", ">="):
        return f"({left} {op} {right})"
    if op == "like":
        return f"({right} in ({left} or ''))"
    if op == "ilike":
        return f"({right}.lower() in ({left} or '').lower())"
    if op == "not like":
        return f"({right} not in ({left} or ''))"
    if op == "not ilike":
        return f"({right}.lower() not in ({left} or '').lower())"
    if op in ("in", "not in"):
        # this is a complex case:
        #  (user_ids, 'in', []) -> empty result
        #  (user_ids, 'in', [2]) -> the result cannot be evaluated as `users_ids == [2]` :/
        # from domain.js:
        # ```
        #  const val = Array.isArray(value) ? value : [value];
        #  const fieldVal = Array.isArray(fieldValue) ? fieldValue : [fieldValue];
        #  return fieldVal.some((fv) => val.includes(fv));
        # ```
        rv = f"{right}" if isinstance(right_ast, (ast.List, ast.Tuple)) else f"[{right}]"
        lv = str(left)
        ttype = target_field_type(cr, model, field_path + left.split("."))
        if isinstance(left, str) and ttype in ("one2many", "many2many"):  # array of ids
            res = f"set({lv}).intersection({rv})"  # odoo/odoo#139827, odoo/odoo#139451
            return f"(not {res})" if op == "not in" else f"({res})"
        else:
            # consider the left-hand side to be a single value
            # ex. ('team_id', 'in', [val1, val2, val3, ...]) => team_id in [val1, val2, val3, ...]
            return f"({lv} {op} {rv})"
    if op in ("=like", "=ilike") and isinstance(right_ast, ast.Constant) and isinstance(right_ast.value, str):
        # this cannot be handled in Python for all cases with the limited support of what
        # can be evaluated in an inline attribute expression, we try to deal with some cases
        # a pattern like 'aaa%bbb%ccc' is imposible to deal with
        pattern = right[1:-1]  # chop the quotes
        lower = ""
        if op == "=ilike":
            pattern = pattern.lower()
            lower = "lower()"
        if "%" not in pattern:
            return f"({left}{lower} == {pattern!r})"
        if pattern.count("%") == 1:  # pattern=aaa%bbbb
            start, end = pattern.split("%")
            return f"({left}{lower}.startswith({start!r}) and {left}{lower}.endswith({end!r}))"
        if pattern.count("%") == 2 and pattern[0] == "%" and pattern[-1] == "%":
            # pattern=%aaa%, same as `like` op with aaa
            pattern = pattern[1:-1]  # chop the %
            return f"({pattern!r} in {left}{lower})"
        # let it fail otherwise
    raise ValueError("Cannot convert leaf to Python (%s, %s, %s)", left, op, right)
