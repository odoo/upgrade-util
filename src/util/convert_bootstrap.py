"""Convert an XML/HTML document Bootstrap code from an older version to a newer one."""

import logging
import os.path
import re
from functools import lru_cache
from typing import Iterable

from lxml import etree

from .misc import parse_version as Version

_logger = logging.getLogger(__name__)


# Regex boundary patterns for class names within attributes (`\b` is not good enough
# because it matches `-` etc.). It's probably enough to detect if the class name
# is surrounded by either whitespace or beginning/end of string.
# Includes boundary chars that can appear in t-att(f)-* attributes.
BS = r"(?:^|(?<=[\s}'\"]))"
BE = r"(?:$|(?=[\s{#'\"]))"
B = rf"(?:{BS}|{BE})"


def _xpath_has_class(context, *cls):
    """Extension function for xpath to check if the context node has all the classes passed as arguments."""
    node_classes = set(context.context_node.attrib.get("class", "").split())
    return node_classes.issuperset(cls)


@lru_cache(maxsize=128)  # does >99% hits over ~10mln calls
def _xpath_has_t_class_inner(attrs_values, classes):
    """
    Inner implementation of :func:`_xpath_has_t_class`, suitable for caching.

    :param tuple[str] attrs_values: the values of the ``t-att-class`` and ``t-attf-class`` attributes
    :param tuple[str] classes: the classes to check
    """
    return any(
        all(re.search(rf"{BS}{escaped_cls}{BE}", attr_value or "") for escaped_cls in map(re.escape, classes))
        for attr_value in attrs_values
    )


def _xpath_has_t_class(context, *cls):
    """Extension function for xpath to check if the context node has all the classes passed as arguments in one of ``class`` or ``t-att(f)-class`` attributes."""
    return _xpath_has_class(context, *cls) or _xpath_has_t_class_inner(
        tuple(map(context.context_node.attrib.get, ("t-att-class", "t-attf-class"))), cls
    )


@lru_cache(maxsize=1024)  # does >88% hits over ~1.5mln calls
def _xpath_regex_inner(pattern, item):
    """Inner implementation of :func:`_xpath_regex`, suitable for caching."""
    return bool(re.search(pattern, item))


def _xpath_regex(context, item, pattern):
    """Extension function for xpath to check if the passed item (attribute or text) matches the passed regex pattern."""
    if not item:
        return False
    if isinstance(item, list):
        item = item[0]  # only first attribute is valid
    return _xpath_regex_inner(pattern, item)


xpath_utils = etree.FunctionNamespace(None)
xpath_utils["hasclass"] = _xpath_has_class
xpath_utils["has-class"] = _xpath_has_class
xpath_utils["has-t-class"] = _xpath_has_t_class
xpath_utils["regex"] = _xpath_regex

html_utf8_parser = etree.HTMLParser(encoding="utf-8")


def innerxml(element, is_html=False):
    """
    Return the inner XML of an element as a string.

    :param etree.ElementBase element: the element to convert.
    :param bool is_html: whether to use HTML for serialization, XML otherwise. Defaults to False.
    :rtype: str
    """
    return (element.text or "") + "".join(
        etree.tostring(child, encoding=str, method="html" if is_html else None) for child in element
    )


def split_classes(*joined_classes):
    """Return a list of classes given one or more strings of joined classes separated by spaces."""
    return [c for classes in joined_classes for c in (classes or "").split(" ") if c]


def get_classes(element):
    """Return the list of classes from the ``class`` attribute of an element."""
    return split_classes(element.get("class", ""))


def join_classes(classes):
    """Return a string of classes joined by space given a list of classes."""
    return " ".join(classes)


def set_classes(element, classes):
    """
    Set the ``class`` attribute of an element from a list of classes.

    If the list is empty, the attribute is removed.
    """
    if classes:
        element.attrib["class"] = join_classes(classes)
    else:
        element.attrib.pop("class", None)


def edit_classlist(classes, add, remove):
    """
    Edit a class list, adding and removing classes.

    :param str | typing.List[str] classes: the original classes list or str to edit.
    :param str | typing.Iterable[str] | None add: if specified, adds the given class(es) to the list.
    :param str | typing.Iterable[str] | ALL | None remove: if specified, removes the given class(es)
        from the list. The `ALL` sentinel value can be specified to remove all classes.
    :rtype: typing.List[str]
    :return: the new class list.
    """
    if remove is ALL:
        classes = []
        remove = None
    else:
        if isinstance(classes, str):
            classes = [classes]
        classes = split_classes(*classes)

    remove_index = None
    if isinstance(remove, str):
        remove = [remove]
    for classname in remove or []:
        while classname in classes:
            remove_index = max(remove_index or 0, classes.index(classname))
            classes.remove(classname)

    insert_index = remove_index if remove_index is not None else len(classes)
    if isinstance(add, str):
        add = [add]
    for classname in add or []:
        if classname not in classes:
            classes.insert(insert_index, classname)
            insert_index += 1

    return classes


def edit_element_t_classes(element, add, remove):
    """
    Edit inplace qweb ``t-att-class`` and ``t-attf-class`` attributes of an element, adding and removing the specified classes.

    N.B. adding new classes will not work if neither ``t-att-class`` nor ``t-attf-class`` are present.

    :param etree.ElementBase element: the element to edit.
    :param str | typing.Iterable[str] | None add: if specified, adds the given class(es) to the element.
    :param str | typing.Iterable[str] | ALL | None remove: if specified, removes the given class(es)
        from the element. The `ALL` sentinel value can be specified to remove all classes.
    """
    if isinstance(add, str):
        add = [add]
    if add:
        add = split_classes(*add)
    if isinstance(remove, str):
        remove = [remove]
    if remove and remove is not ALL:
        remove = split_classes(*remove)

    if not add and not remove:
        return  # nothing to do

    for attr in ("t-att-class", "t-attf-class"):
        if attr in element.attrib:
            value = element.attrib.pop(attr)

            # if we need to remove all classes, just remove or replace the attribute
            # with the literal string of classes to add, removing all logic
            if remove is ALL:
                if not add:
                    value = None
                else:
                    value = " ".join(add or [])
                    if attr.startswith("t-att-"):
                        value = f"'{value}'"

            else:
                joined_adds = join_classes(add or [])
                # if there's no classes to remove, try to append the new classes in a sane way
                if not remove:
                    if add:
                        if attr.startswith("t-att-"):
                            value = f"{value} + ' {joined_adds}'" if value else f"'{joined_adds}'"
                        else:
                            value = f"{value} {joined_adds}"
                # otherwise use regexes to replace the classes in the attribute
                # if there's just one replace, do a string replacement with the new classes
                elif len(remove) == 1:
                    value = re.sub(rf"{BS}{re.escape(remove[0])}{BE}", joined_adds, value)
                # else there's more than one class to remove and split at matches,
                # then rejoin with new ones at the position of the last removed one.
                else:
                    value = re.split(rf"{BS}(?:{'|'.join(map(re.escape, remove))}){BE}", value)
                    value = "".join(value[:-1] + [joined_adds] + value[-1:])

            if value is not None:
                element.attrib[attr] = value


def edit_element_classes(element, add, remove, is_qweb=False):
    """
    Edit inplace the "class" attribute of an element, adding and removing classes.

    :param etree.ElementBase element: the element to edit.
    :param str | typing.Iterable[str] | None add: if specified, adds the given class(es) to the element.
    :param str | typing.Iterable[str] | ALL | None remove: if specified, removes the given class(es)
        from the element. The `ALL` sentinel value can be specified to remove all classes.
    :param bool is_qweb: if True also edit classes in ``t-att-class`` and ``t-attf-class`` attributes.
        Defaults to False.
    """
    if not is_qweb or not set(element.attrib) & {"t-att-class", "t-attf-class"}:
        set_classes(element, edit_classlist(get_classes(element), add, remove))
    if is_qweb:
        edit_element_t_classes(element, add, remove)


ALL = object()
"""Sentinel object to indicate "all items" in a collection"""


def simple_css_selector_to_xpath(selector, prefix="//"):
    """
    Convert a basic CSS selector cases to an XPath expression.

    Supports node names, classes, ``>`` and ``,`` combinators.

    :param str selector: the CSS selector to convert.
    :param str prefix: the prefix to add to the XPath expression. Defaults to ``//``.
    :return: the resulting XPath expression.
    :rtype: str
    """
    separator = prefix
    xpath_parts = []
    combinators = "+>,~ "
    for selector_part in map(str.strip, re.split(rf"(\s*[{combinators}]\s*)", selector)):
        if not selector_part:
            separator = "//"
        elif selector_part == ">":
            separator = "/"
        elif selector_part == ",":
            separator = "|" + prefix
        elif re.search(r"^(?:[a-z](-?\w+)*|[*.])", selector_part, flags=re.I):
            element, *classes = selector_part.split(".")
            if not element:
                element = "*"
            class_predicates = [f"[hasclass('{classname}')]" for classname in classes if classname]
            xpath_parts += [separator, element + "".join(class_predicates)]
        else:
            raise NotImplementedError(f"Unsupported CSS selector syntax: {selector}")

    return "".join(xpath_parts)


CSS = simple_css_selector_to_xpath


def regex_xpath(pattern, attr=None, xpath=None):
    """
    Return an XPath expression that matches elements with an attribute value matching the given regex pattern.

    :param str pattern: a regex pattern to match the attribute value against.
    :param str | None attr: the attribute to match the pattern against.
        If not given, the pattern is matched against the element's text.
    :param str | None xpath: an optional XPath expression to further filter the elements to match.
    :rtype: str
    """
    # TODO abt: investigate lxml xpath variables interpolation (xpath.setcontext? registerVariables?)
    if "'" in pattern and '"' in pattern:
        quoted_pattern = "concat('" + "', \"'\", '".join(pattern.split("'")) + "')"
    elif "'" in pattern:
        quoted_pattern = '"' + pattern + '"'
    else:
        quoted_pattern = "'" + pattern + "'"
    xpath_pre = xpath or "//*"
    attr_or_text = f"@{attr}" if attr is not None else "text()"
    return xpath_pre + f"[regex({attr_or_text}, {quoted_pattern})]"


def adapt_xpath_for_qweb(xpath):
    """Adapts a xpath to enable matching on qweb ``t-att(f)-`` attributes."""
    xpath = re.sub(r"\bhas-?class(?=\()", "has-t-class", xpath)
    # supposing that there's only one of `class`, `t-att-class`, `t-attf-class`,
    # joining all of them with a space and removing trailing whitespace should behave
    # similarly to COALESCE, and result in ORing the values for matching
    xpath = re.sub(
        r"(?<=\()\s*@(?<!t-)([\w-]+)",
        r"normalize-space(concat(@\1, ' ', @t-att-\1, ' ', @t-attf-\1))",
        xpath,
    )
    return re.sub(r"\[@(?<!t-)([\w-]+)\]", r"[@\1 or @t-att-\1 or @t-attf-\1]", xpath)


class ElementOperation:
    """Abstract base class for defining operations to be applied on etree elements."""

    def __call__(self, element, converter):
        """
        Perform the operation on the given element.

        Abstract method that must be implemented by subclasses.

        :param etree.ElementBase element: the etree element to apply the operation on.
        :param BootstrapConverter converter: the converter that's operating on the etree document.
        :return: the converted element, which could be the same provided, a different one, or None.
            The returned element should be used by the converter to chain further operations.
        :rtype: etree.ElementBase | None
        """
        raise NotImplementedError

    def xpath(self, xpath=None):
        """
        Return an XPath expression that matches elements for the operation.

        :param str | None xpath: an optional XPath expression to further filter the elements to match.
        :rtype: str
        """
        raise NotImplementedError(f"Operation {self.__class__.__name__} does not support XPath matching")

    @classmethod
    def op(cls, *args, xpath=None, **kwargs):
        """
        Create a definition of an operation with the given arguments, and returns a tuple of (xpath, operations list) that can be used in the converter definition list.

        :param typing.Any args: positional arguments to pass to the operation :meth:`~.__init__`.
        :param typing.Any kwargs: keyword arguments to pass to the operation :meth:`~.__init__`.
        :param str | None xpath: an optional XPath expression to further filter the elements to match.
        :rtype: (str, list[ElementOperation])
        """
        op = cls(*args, **kwargs)
        return op.xpath(xpath), [op]


class RemoveElement(ElementOperation):
    """Remove the matched element(s) from the document."""

    def __call__(self, element, converter):
        parent = element.getparent()
        if parent is None:
            raise ValueError(f"Cannot remove root element {element}")
        parent.remove(element)


class EditClasses(ElementOperation):
    """
    Add and/or remove classes.

    :param str | typing.Iterable[str] | None add: classes to add to the elements.
    :param str | typing.Iterable[str] | ALL | None remove: classes to remove from the elements.
        The `ALL` sentinel can be used to remove all classes.
    """

    def __init__(self, *, add=None, remove=None):
        if not add and not remove:
            raise ValueError("At least one of `add` or `remove` must be specified")
        if isinstance(add, str):
            add = [add]
        if isinstance(remove, str):
            remove = [remove]
        self.add_classes = split_classes(*add) if isinstance(add, Iterable) else add
        self.remove_classes = split_classes(*remove) if isinstance(remove, Iterable) else remove

    def __call__(self, element, converter):
        edit_element_classes(element, self.add_classes, self.remove_classes, is_qweb=converter.is_qweb)
        return element

    def xpath(self, xpath=None):
        """
        Return an XPath expression that matches elements with any of the old classes.

        :param str | None xpath: an optional XPath expression to further filter the elements to match.
        :rtype: str
        """
        if not self.remove_classes:
            raise ValueError("Cannot generate an XPath expression without any classes to remove (i.e. match on)")
        xpath_pre = xpath or "//*"
        if self.remove_classes is ALL:
            if not xpath:
                raise ValueError(
                    f"{self.__class__.__name__}: Attempted to generate XPath matching any class for any element. "
                    "Provide an additional `xpath=` argument to narrow down the elements to match."
                )
            return xpath_pre + "[@class]"
        return xpath_pre + ("[" + " or ".join(f"hasclass('{c}')" for c in self.remove_classes) + "]")


class AddClasses(EditClasses):
    def __init__(self, *classes):
        super().__init__(add=classes)

    xpath = ElementOperation.xpath


class RemoveClasses(EditClasses):
    """
    Remove classes.

    N.B. no checks are made to ensure the class(es) to remove are actually present on the elements.
    """

    def __init__(self, *classes):
        super().__init__(remove=classes)


class ReplaceClasses(EditClasses):
    """
    Replace old classes with new ones.

    :param str | typing.Iterable[str] | ALL old: classes to remove from the elements.
    :param str | typing.Iterable[str] | None new: classes to add to the elements.
    """

    def __init__(self, old, new):
        if not old:
            raise ValueError("At least one class to remove must be specified")
        super().__init__(remove=old, add=new)


class PullUp(ElementOperation):
    """
    Pull up the element's children to the parent element, then removes the original element.

    Example:
    -------
    before::
     .. code-block:: html

        <div class="input-group-prepend">
            <span class="input-group-text">...</span>
        </div>

    after::
     .. code-block:: html

        <span class="input-group-text">...</span>

    """

    def __call__(self, element, converter):
        parent = element.getparent()
        if parent is None:
            raise ValueError(f"Cannot pull up contents of xml element with no parent: {element}")

        prev_sibling = element.getprevious()
        if prev_sibling is not None:
            prev_sibling.tail = ((prev_sibling.tail or "") + (element.text or "")) or None
        else:
            parent.text = ((parent.text or "") + (element.text or "")) or None

        for child in element:
            element.addprevious(child)

        parent.remove(element)


class RenameAttribute(ElementOperation):
    """Rename an attribute. Silently ignores elements that do not have the attribute."""

    def __init__(self, old_name, new_name, extra_xpath=""):
        self.old_name = old_name
        self.new_name = new_name
        self.extra_xpath = extra_xpath

    def __call__(self, element, converter):
        rename_map = {self.old_name: self.new_name}
        if converter.is_qweb:
            rename_map = {
                f"{prefix}{old}": f"{prefix}{new}"
                for old, new in rename_map.items()
                for prefix in ("",) + (("t-att-", "t-attf-") if not old.startswith("t-") else ())
                if f"{prefix}{old}" in element.attrib
            }
        if rename_map:
            # to preserve attributes order, iterate+rename on a copy and reassign (clear+update, bc readonly property)
            attrib_before = dict(element.attrib)
            element.attrib.clear()
            element.attrib.update({rename_map.get(k, k): v for k, v in attrib_before.items()})
        return element

    def xpath(self, xpath=None):
        """
        Return an XPath expression that matches elements with the old attribute name.

        :param str | None xpath: an optional XPath expression to further filter the elements to match.
        :rtype: str
        """
        return (xpath or "//*") + f"[@{self.old_name}]{self.extra_xpath}"


class RegexReplace(ElementOperation):
    """
    Uses `re.sub` to modify an attribute or the text of an element.

    N.B. no checks are made to ensure the attribute to replace is actually present on the elements.

    :param str pattern: the regex pattern to match.
    :param str repl: the replacement string.
    :param str | None attr: the attribute to replace. If not specified, the text of the element is replaced.
    """

    def __init__(self, pattern, sub, attr=None):
        self.pattern = pattern
        self.repl = sub
        self.attr = attr

    def __call__(self, element, converter):
        if self.attr is None:
            # TODO abt: what about tail?
            element.text = re.sub(self.pattern, self.repl, element.text or "")
        else:
            for attr in (self.attr,) + ((f"t-att-{self.attr}", f"t-attf-{self.attr}") if converter.is_qweb else ()):
                if attr in element.attrib:
                    element.attrib[attr] = re.sub(self.pattern, self.repl, element.attrib[attr])
        return element

    def xpath(self, xpath=None):
        """
        Return an XPath expression that matches elements with the old attribute name.

        :param str | None xpath: an optional XPath expression to further filter the elements to match.
        :rtype: str
        """
        return regex_xpath(self.pattern, self.attr, xpath)


class RegexReplaceClass(RegexReplace):
    """
    Uses `re.sub` to modify the class.

    Basically, same as `RegexReplace`, but with `attr="class"`.
    """

    def __init__(self, pattern, sub, attr="class"):
        super().__init__(pattern, sub, attr)


class BS3to4ConvertBlockquote(ElementOperation):
    """Convert a BS3 ``<blockquote>`` element to a BS4 ``<div>`` element with the ``blockquote`` class."""

    def __call__(self, element, converter):
        blockquote = converter.copy_element(element, tag="div", add_classes="blockquote", copy_attrs=False)
        element.addnext(blockquote)
        element.getparent().remove(element)
        return blockquote


# TODO abt: merge MakeCard and ConvertCard into one operation class
class BS3to4MakeCard(ElementOperation):
    """
    Pre-processe a BS3 panel, thumbnail, or well element to be converted to a BS4 card.

    Card components conversion is then handled by the ``ConvertCard`` operation class.
    """

    def __call__(self, element, converter):
        card = converter.element_factory("<div class='card'/>")
        card_body = converter.copy_element(
            element, tag="div", add_classes="card-body", remove_classes=ALL, copy_attrs=False
        )
        card.append(card_body)
        element.addnext(card)
        element.getparent().remove(element)
        return card


# TODO abt: refactor code
class BS3to4ConvertCard(ElementOperation):
    """Fully convert a BS3 panel, thumbnail, or well element and their contents to a BS4 card."""

    POST_CONVERSIONS = {
        "title": ["card-title"],
        "description": ["card-description"],
        "category": ["card-category"],
        "panel-danger": ["card", "bg-danger", "text-white"],
        "panel-warning": ["card", "bg-warning"],
        "panel-info": ["card", "bg-info", "text-white"],
        "panel-success": ["card", "bg-success", "text-white"],
        "panel-primary": ["card", "bg-primary", "text-white"],
        "panel-footer": ["card-footer"],
        "panel-body": ["card-body"],
        "panel-title": ["card-title"],
        "panel-heading": ["card-header"],
        "panel-default": [],
        "panel": ["card"],
    }

    def _convert_child(self, child, old_card, new_card, converter):
        old_card_classes = get_classes(old_card)

        classes = get_classes(child)

        if "header" in classes or ("image" in classes and len(child)):
            add_classes = "card-header"
            remove_classes = ["header", "image"]
        elif "content" in classes:
            add_classes = "card-img-overlay" if "card-background" in old_card_classes else "card-body"
            remove_classes = "content"
        elif {"card-footer", "footer", "text-center"} & set(classes):
            add_classes = "card-footer"
            remove_classes = "footer"
        else:
            new_card.append(child)
            return

        new_child = converter.copy_element(
            child, "div", add_classes=add_classes, remove_classes=remove_classes, copy_attrs=True
        )

        if "image" in classes:
            [img_el] = new_child.xpath("./img")[:1] or [None]
            if img_el is not None and "src" in img_el:
                new_child.attrib["style"] = (
                    f'background-image: url("{img_el.attrib["src"]}"); '
                    "background-position: center center; "
                    "background-size: cover;"
                )
                new_child.remove(img_el)

        new_card.append(new_child)

        if "content" in classes:  # TODO abt: consider skipping for .card-background
            [footer] = new_child.xpath(converter.adapt_xpath("./*[hasclass('footer')]"))[:1] or [None]
            if footer is not None:
                self._convert_child(footer, old_card, new_card, converter)
                new_child.remove(footer)

    def _postprocess(self, new_card, converter):
        for old_class, new_classes in self.POST_CONVERSIONS.items():
            for element in new_card.xpath(converter.adapt_xpath(f"(.|.//*)[hasclass('{old_class}')]")):
                edit_element_classes(element, add=new_classes, remove=old_class)

    def __call__(self, element, converter):
        classes = get_classes(element)
        new_card = converter.copy_element(element, tag="div", copy_attrs=True, copy_contents=False)
        wrapper = new_card
        if "card-horizontal" in classes:
            wrapper = etree.SubElement(new_card, "div", {"class": "row"})

        for child in element:
            self._convert_child(child, element, wrapper, converter)

        self._postprocess(new_card, converter)
        element.addnext(new_card)
        element.getparent().remove(element)
        return new_card


class BS4to5ConvertCloseButton(ElementOperation):
    """
    Convert BS4 ``button.close`` elements to BS5 ``button.btn-close``.

    Also fixes the ``data-dismiss`` attribute to ``data-bs-dismiss``, and removes any inner contents.
    """

    def __call__(self, element, converter):
        new_btn = converter.copy_element(element, remove_classes="close", add_classes="btn-close", copy_contents=False)

        if "data-dismiss" in element.attrib:
            new_btn.attrib["data-bs-dismiss"] = element.attrib["data-dismiss"]
            del new_btn.attrib["data-dismiss"]

        element.addnext(new_btn)
        element.getparent().remove(element)

        return new_btn


class BS4to5ConvertCardDeck(ElementOperation):
    """Convert BS4 ``.card-deck`` elements to grid components (``.row``, ``.col``, etc.)."""

    def __call__(self, element, converter):
        cards = element.xpath(converter.adapt_xpath("./*[hasclass('card')]"))

        cols_class = f"row-cols-{len(cards)}" if len(cards) in range(1, 7) else "row-cols-auto"
        edit_element_classes(element, add=["row", cols_class], remove="card-deck", is_qweb=converter.is_qweb)

        for card in cards:
            new_col = converter.build_element("div", classes=["col"])
            card.addprevious(new_col)
            new_col.append(card)

        return element


class BS4to5ConvertFormInline(ElementOperation):
    """Convert BS4 ``.form-inline`` elements to grid components (``.row``, ``.col``, etc.)."""

    def __call__(self, element, converter):
        edit_element_classes(element, add="row row-cols-lg-auto", remove="form-inline", is_qweb=converter.is_qweb)

        children_selector = converter.adapt_xpath(
            CSS(".form-control,.form-group,.form-check,.input-group,.custom-select,button", prefix="./")
        )
        indexed_children = sorted([(element.index(c), c) for c in element.xpath(children_selector)], key=lambda x: x[0])

        nest_groups = []
        last_idx = -1
        for idx, child in indexed_children:
            nest_start, nest_end = idx, idx
            labels = [label for label in child.xpath("preceding-sibling::label") if element.index(label) > last_idx]
            labels = [
                label
                for label in labels
                if "for" in label.attrib and child.xpath(f"descendant-or-self::*[@id='{label.attrib['for']}']")
            ] or labels[-1:]
            if labels:
                first_label = labels[0]
                assert last_idx < element.index(first_label) < idx, "label must be between last group and current"
                nest_start = element.index(first_label)

            assert nest_start <= nest_end, f"expected start {nest_start} to be <= end {nest_end}"
            nest_groups.append(element[nest_start : nest_end + 1])
            last_idx = nest_end

        for els in nest_groups:
            wrapper = converter.build_element("div", classes=["col-12"])
            els[0].addprevious(wrapper)
            for el in els:
                wrapper.append(el)
                assert el not in element, f"expected {el!r} to be removed from {element!r}"

        return element


class BootstrapConverter:
    """
    Class for converting XML or HTML Bootstrap code across versions.

    :param etree.ElementTree tree: the parsed XML or HTML tree to convert.
    :param bool is_html: whether the tree is an HTML document.
    """

    MIN_VERSION = "3.0"
    """Minimum supported Bootstrap version."""

    # Conversions definitions by destination version.
    # It's a dictionary of version strings to a list of (xpath, operations_list) tuples.
    # For operations that implement the `xpath()` method, the `op()` class method can be used
    # to directly define the operation and return the tuple with the corresponding XPath expression
    # and the operation list.
    # The `convert()` method will then process the conversions list in order, and for each tuple
    # match the elements in the tree using the XPath expression and apply the operations list to them.
    CONVERSIONS = {
        "4.0": [
            # inputs
            (CSS(".form-group .control-label"), [ReplaceClasses("control-label", "form-control-label")]),
            (CSS(".form-group .text-help"), [ReplaceClasses("text-help", "form-control-feedback")]),
            (CSS(".control-group .help-block"), [ReplaceClasses("help-block", "form-text")]),
            ReplaceClasses.op("form-group-sm", "form-control-sm"),
            ReplaceClasses.op("form-group-lg", "form-control-lg"),
            (CSS(".form-control .input-sm"), [ReplaceClasses("input-sm", "form-control-sm")]),
            (CSS(".form-control .input-lg"), [ReplaceClasses("input-lg", "form-control-lg")]),
            # hide
            ReplaceClasses.op("hidden-xs", "d-none"),
            ReplaceClasses.op("hidden-sm", "d-sm-none"),
            ReplaceClasses.op("hidden-md", "d-md-none"),
            ReplaceClasses.op("hidden-lg", "d-lg-none"),
            ReplaceClasses.op("visible-xs", "d-block d-sm-none"),
            ReplaceClasses.op("visible-sm", "d-block d-md-none"),
            ReplaceClasses.op("visible-md", "d-block d-lg-none"),
            ReplaceClasses.op("visible-lg", "d-block d-xl-none"),
            # image
            ReplaceClasses.op("img-rounded", "rounded"),
            ReplaceClasses.op("img-circle", "rounded-circle"),
            ReplaceClasses.op("img-responsive", ("d-block", "img-fluid")),
            # buttons
            ReplaceClasses.op("btn-default", "btn-secondary"),
            ReplaceClasses.op("btn-xs", "btn-sm"),
            (CSS(".btn-group.btn-group-xs"), [ReplaceClasses("btn-group-xs", "btn-group-sm")]),
            (CSS(".dropdown .divider"), [ReplaceClasses("divider", "dropdown-divider")]),
            ReplaceClasses.op("badge", "badge badge-pill"),
            ReplaceClasses.op("label", "badge"),
            RegexReplaceClass.op(rf"{BS}label-(default|primary|success|info|warning|danger){BE}", r"badge-\1"),
            (CSS(".breadcrumb > li"), [ReplaceClasses("breadcrumb", "breadcrumb-item")]),
            # li
            (CSS(".list-inline > li"), [AddClasses("list-inline-item")]),
            # pagination
            (CSS(".pagination > li"), [AddClasses("page-item")]),
            (CSS(".pagination > li > a"), [AddClasses("page-link")]),
            # carousel
            (CSS(".carousel .carousel-inner > .item"), [ReplaceClasses("item", "carousel-item")]),
            # pull
            ReplaceClasses.op("pull-right", "float-right"),
            ReplaceClasses.op("pull-left", "float-left"),
            ReplaceClasses.op("center-block", "mx-auto"),
            # well
            (CSS(".well"), [BS3to4MakeCard()]),
            (CSS(".thumbnail"), [BS3to4MakeCard()]),
            # blockquote
            (CSS("blockquote"), [BS3to4ConvertBlockquote()]),
            (CSS(".blockquote.blockquote-reverse"), [ReplaceClasses("blockquote-reverse", "text-right")]),
            # dropdown
            (CSS(".dropdown-menu > li > a"), [AddClasses("dropdown-item")]),
            (CSS(".dropdown-menu > li"), [PullUp()]),
            # in
            ReplaceClasses.op("in", "show"),
            # table
            (CSS("tr.active, td.active"), [ReplaceClasses("active", "table-active")]),
            (CSS("tr.success, td.success"), [ReplaceClasses("success", "table-success")]),
            (CSS("tr.info, td.info"), [ReplaceClasses("info", "table-info")]),
            (CSS("tr.warning, td.warning"), [ReplaceClasses("warning", "table-warning")]),
            (CSS("tr.danger, td.danger"), [ReplaceClasses("danger", "table-danger")]),
            (CSS("table.table-condesed"), [ReplaceClasses("table-condesed", "table-sm")]),
            # navbar
            (CSS(".nav.navbar > li > a"), [AddClasses("nav-link")]),
            (CSS(".nav.navbar > li"), [AddClasses("nav-intem")]),
            ReplaceClasses.op("navbar-btn", "nav-item"),
            (CSS(".navbar-nav"), [ReplaceClasses("navbar-right nav", "ml-auto")]),
            ReplaceClasses.op("navbar-toggler-right", "ml-auto"),
            (CSS(".navbar-nav > li > a"), [AddClasses("nav-link")]),
            (CSS(".navbar-nav > li"), [AddClasses("nav-item")]),
            (CSS(".navbar-nav > a"), [AddClasses("navbar-brand")]),
            ReplaceClasses.op("navbar-fixed-top", "fixed-top"),
            ReplaceClasses.op("navbar-toggle", "navbar-toggler"),
            ReplaceClasses.op("nav-stacked", "flex-column"),
            (CSS("nav.navbar"), [AddClasses("navbar-expand-lg")]),
            (CSS("button.navbar-toggle"), [ReplaceClasses("navbar-toggle", "navbar-expand-md")]),
            # card
            (CSS(".panel"), [BS3to4ConvertCard()]),
            (CSS(".card"), [BS3to4ConvertCard()]),
            # grid
            RegexReplaceClass.op(rf"{BS}col((?:-\w{{2}})?)-offset-(\d{{1,2}}){BE}", r"offset\1-\2"),
        ],
        "5.0": [
            # links
            RegexReplaceClass.op(rf"{BS}text-(?!o-)", "link-", xpath=CSS("a")),
            (CSS(".nav-item.active > .nav-link"), [AddClasses("active")]),
            (CSS(".nav-link.active") + CSS(".nav-item.active", prefix="/parent::"), [RemoveClasses("active")]),
            # badges
            ReplaceClasses.op("badge-pill", "rounded-pill"),
            RegexReplaceClass.op(rf"{BS}badge-", r"text-bg-"),
            # buttons
            ("//*[hasclass('btn-block')]/parent::div", [AddClasses("d-grid gap-2")]),
            ("//*[hasclass('btn-block')]/parent::p[count(./*)=1]", [AddClasses("d-grid gap-2")]),
            RemoveClasses.op("btn-block"),
            (CSS("button.close"), [BS4to5ConvertCloseButton()]),
            # card
            # TODO abt: .card-columns (unused in odoo)
            (CSS(".card-deck"), [BS4to5ConvertCardDeck()]),
            # jumbotron
            ReplaceClasses.op("jumbotron", "container-fluid py-5"),
            # new data-bs- attributes
            RenameAttribute.op("data-display", "data-bs-display", "[not(@data-snippet='s_countdown')]"),
            *[
                RenameAttribute.op(f"data-{attr}", f"data-bs-{attr}")
                for attr in [
                    "animation",
                    "attributes",
                    "autohide",
                    "backdrop",
                    "body",
                    "container",
                    "content",
                    "delay",
                    "dismiss",
                    "focus",
                    "interval",
                    "margin-right",
                    "no-jquery",
                    "offset",
                    "original-title",
                    "padding-right",
                    "parent",
                    "placement",
                    "ride",
                    "sanitize",
                    "show",
                    "slide",
                    "slide-to",
                    "spy",
                    "target",
                    "toggle",
                    "touch",
                    "trigger",
                    "whatever",
                ]
            ],
            # popover
            (CSS(".popover .arrow"), [ReplaceClasses("arrow", "popover-arrow")]),
            # form
            ReplaceClasses.op("form-row", "row"),
            ("//*[hasclass('form-group')]/parent::form", [AddClasses("row")]),
            ReplaceClasses.op("form-group", "col-12 py-2"),
            (CSS(".form-inline"), [BS4to5ConvertFormInline()]),
            ReplaceClasses.op("custom-checkbox", "form-check"),
            RegexReplaceClass.op(rf"{BS}custom-control-(input|label){BE}", r"form-check-\1"),
            RegexReplaceClass.op(rf"{BS}custom-control-(input|label){BE}", r"form-check-\1"),
            RegexReplaceClass.op(rf"{BS}custom-(check|select|range){BE}", r"form-\1"),
            (CSS(".custom-switch"), [ReplaceClasses("custom-switch", "form-check form-switch")]),
            ReplaceClasses.op("custom-radio", "form-check"),
            RemoveClasses.op("custom-control"),
            (CSS(".custom-file"), [PullUp()]),
            RegexReplaceClass.op(rf"{BS}custom-file-", r"form-file-"),
            RegexReplaceClass.op(rf"{BS}form-file(?:-input)?{BE}", r"form-control"),
            (CSS("label.form-file-label"), [RemoveElement()]),
            (regex_xpath(rf"{BS}input-group-(prepend|append){BE}", "class"), [PullUp()]),
            ("//label[not(hasclass('form-check-label'))]", [AddClasses("form-label")]),
            ReplaceClasses.op("form-control-file", "form-control"),
            ReplaceClasses.op("form-control-range", "form-range"),
            # TODO abt: .form-text no longer sets display, add some class?
            # table
            RegexReplaceClass.op(rf"{BS}thead-(light|dark){BE}", r"table-\1"),
            # grid
            RegexReplaceClass.op(rf"{BS}col-((?:\w{{2}}-)?)offset-(\d{{1,2}}){BE}", r"offset-\1\2"),  # from BS4
            # gutters
            ReplaceClasses.op("no-gutters", "g-0"),
            # logical properties
            RegexReplaceClass.op(rf"{BS}left-((?:\w{{2,3}}-)?[0-9]+|auto){BE}", r"start-\1"),
            RegexReplaceClass.op(rf"{BS}right-((?:\w{{2,3}}-)?[0-9]+|auto){BE}", r"end-\1"),
            RegexReplaceClass.op(rf"{BS}((?:float|border|rounded|text)(?:-\w+)?)-left{BE}", r"\1-start"),
            RegexReplaceClass.op(rf"{BS}((?:float|border|rounded|text)(?:-\w+)?)-right{BE}", r"\1-end"),
            RegexReplaceClass.op(rf"{BS}rounded-sm(-(?:start|end|top|bottom))?", r"rounded\1-1"),
            RegexReplaceClass.op(rf"{BS}rounded-lg(-(?:start|end|top|bottom))?", r"rounded\1-3"),
            RegexReplaceClass.op(rf"{BS}([mp])l-((?:\w{{2,3}}-)?(?:[0-9]+|auto)){BE}", r"\1s-\2"),
            RegexReplaceClass.op(rf"{BS}([mp])r-((?:\w{{2,3}}-)?(?:[0-9]+|auto)){BE}", r"\1e-\2"),
            ReplaceClasses.op("dropdown-menu-left", "dropdown-menu-start"),
            ReplaceClasses.op("dropdown-menu-right", "dropdown-menu-end"),
            ReplaceClasses.op("dropleft", "dropstart"),
            ReplaceClasses.op("dropright", "dropend"),
            # tooltips
            (
                "//*[hasclass('tooltip') or @role='tooltip']//*[hasclass('arrow')]",
                [ReplaceClasses("arrow", "tooltip-arrow")],
            ),
            # utilities
            ReplaceClasses.op("text-monospace", "font-monospace"),
            RegexReplaceClass.op(rf"{BS}font-weight-", r"fw-"),
            RegexReplaceClass.op(rf"{BS}font-style-", r"fst-"),
            ReplaceClasses.op("font-italic", "fst-italic"),
            # helpers
            RegexReplaceClass.op(rf"{BS}embed-responsive-(\d+)by(\d+)", r"ratio-\1x\2"),
            RegexReplaceClass.op(rf"{BS}ratio-(\d+)by(\d+)", r"ratio-\1x\2"),
            RegexReplaceClass.op(rf"{BS}embed-responsive(?!-)", r"ratio"),
            RegexReplaceClass.op(rf"{BS}sr-only(-focusable)?", r"visually-hidden\1"),
            # media
            ReplaceClasses.op("media-body", "flex-grow-1"),
            ReplaceClasses.op("media", "d-flex"),
        ],
    }

    def __init__(self, tree, is_html=False, is_qweb=False):
        self.tree = tree
        self.is_html = is_html
        self.is_qweb = is_qweb

    @classmethod
    def _get_sorted_conversions(cls):
        """Return the conversions dict sorted by version, from oldest to newest."""
        return sorted(cls.CONVERSIONS.items(), key=lambda kv: Version(kv[0]))

    @classmethod
    @lru_cache(maxsize=8)
    def get_conversions(cls, src_ver, dst_ver, is_qweb=False):
        """
        Return the list of conversions to convert Bootstrap from ``src_ver`` to ``dst_ver``, with compiled XPaths.

        :param str src_ver: the source Bootstrap version.
        :param str dst_ver: the destination Bootstrap version.
        :param bool is_qweb: whether to adapt conversions for QWeb (to support ``t-att(f)-`` conversions).
        :rtype: list[(etree.XPath, list[ElementOperation])]
        """
        if Version(dst_ver) < Version(src_ver):
            raise NotImplementedError("Downgrading Bootstrap versions is not supported.")
        if Version(src_ver) < Version(cls.MIN_VERSION):
            raise NotImplementedError(f"Conversion from Bootstrap version {src_ver} is not supported")
        result = []
        for version, conversions in BootstrapConverter._get_sorted_conversions():
            if Version(src_ver) < Version(version) <= Version(dst_ver):
                result.extend(conversions)
        if not result:
            if Version(src_ver) == Version(dst_ver):
                _logger.info("Source and destination versions are the same, no conversion needed.")
            else:
                raise NotImplementedError(f"Conversion from {src_ver} to {dst_ver} is not supported")
        if is_qweb:
            result = [(adapt_xpath_for_qweb(xpath), conversions) for xpath, conversions in result]
        return [(etree.XPath(xpath), conversions) for xpath, conversions in result]

    def convert(self, src_version, dst_version):
        """
        Convert the loaded document inplace from the source version to the destination, returning the converted document and the number of conversion operations applied.

        :param str src_version: the source Bootstrap version.
        :param str dst_version: the destination Bootstrap version.
        :rtype: etree.ElementTree, int
        """
        conversions = self.get_conversions(src_version, dst_version, is_qweb=self.is_qweb)
        applied_operations_count = 0
        for xpath, operations in conversions:
            for element in xpath(self.tree):
                for operation in operations:
                    if element is None:  # previous operations that returned None (i.e. deleted element)
                        raise ValueError("Matched xml element is not available anymore! Check operations.")
                    element = operation(element, self)  # noqa: PLW2901
                    applied_operations_count += 1
        return self.tree, applied_operations_count

    @classmethod
    def convert_arch(cls, arch, src_version, dst_version, is_html=False, **converter_kwargs):
        """
        Class method for converting a string of XML or HTML code.

        :param str arch: the XML or HTML code to convert.
        :param str src_version: the source Bootstrap version.
        :param str dst_version: the destination Bootstrap version.
        :param bool is_html: whether the arch is an HTML document.
        :param dict converter_kwargs: additional keyword arguments to pass to the converter.
        :return: the converted XML or HTML code.
        :rtype: str
        """
        stripped_arch = arch.strip()
        doc_header_match = re.search(r"^<\?xml .+\?>\s*", stripped_arch)
        doc_header = doc_header_match.group(0) if doc_header_match else ""
        stripped_arch = stripped_arch[doc_header_match.end() :] if doc_header_match else stripped_arch

        tree = etree.fromstring(f"<wrap>{stripped_arch}</wrap>", parser=html_utf8_parser if is_html else None)

        tree, ops_count = cls(tree, is_html, **converter_kwargs).convert(src_version, dst_version)
        if not ops_count:
            return arch

        wrap_node = tree.xpath("//wrap")[0]
        return doc_header + "\n".join(
            etree.tostring(child, encoding="unicode", with_tail=True, method="html" if is_html else None)
            for child in wrap_node
        )

    @classmethod
    def convert_file(cls, path, src_version, dst_version, is_html=None, **converter_kwargs):
        """
        Class method for converting an XML or HTML file inplace.

        :param str path: the path to the XML or HTML file to convert.
        :param str src_version: the source Bootstrap version.
        :param str dst_version: the destination Bootstrap version.
        :param bool is_html: whether the file is an HTML document.
            If not set, will be detected from the file extension.
        :param dict converter_kwargs: additional keyword arguments to pass to the converter.
        :rtype: None
        """
        if is_html is None:
            is_html = os.path.splitext(path)[1].startswith("htm")
        tree = etree.parse(path, parser=html_utf8_parser if is_html else None)

        tree, ops_count = cls(tree, is_html, **converter_kwargs).convert(src_version, dst_version)
        if not ops_count:
            logging.info("No conversion operations applied, skipping file %s", path)
            return

        tree.write(path, encoding="utf-8", method="html" if is_html else None, xml_declaration=not is_html)

    def element_factory(self, *args, **kwargs):
        """
        Create new elements using the correct document type.

        Basically a wrapper for either etree.XML or etree.HTML depending on the type of document loaded.

        :param args: positional arguments to pass to the etree.XML or etree.HTML function.
        :param kwargs: keyword arguments to pass to the etree.XML or etree.HTML function.
        :return: the created element.
        """
        return etree.HTML(*args, **kwargs) if self.is_html else etree.XML(*args, **kwargs)

    def build_element(self, tag, classes=None, contents=None, **attributes):
        """
        Create a new element with the given tag, classes, contents and attributes.

        Like :meth:`~.element_factory`, can be used by operations to create elements abstracting away the document type.

        :param str tag: the tag of the element to create.
        :param typing.Iterable[str] | None classes: the classes to set on the new element.
        :param str | None contents: the contents of the new element (i.e. inner text/HTML/XML).
        :param dict[str, str] attributes: attributes to set on the new element, provided as keyword arguments.
        :return: the created element.
        :rtype: etree.ElementBase
        """
        element = self.element_factory(f"<{tag}>{contents or ''}</{tag}>")
        for name, value in attributes.items():
            element.attrib[name] = value
        if classes:
            set_classes(element, classes)
        return element

    def copy_element(
        self,
        element,
        tag=None,
        add_classes=None,
        remove_classes=None,
        copy_attrs=True,
        copy_contents=True,
        **attributes,
    ):
        """
        Create a copy of an element, optionally changing the tag, classes, contents and attributes.

        Like :meth:`~.element_factory`, can be used by operations to copy elements abstracting away the document type.

        :param etree.ElementBase element: the element to copy.
        :param str | None tag: if specified, overrides the tag of the new element.
        :param str | typing.Iterable[str] | None add_classes: if specified, adds the given class(es) to the new element.
        :param str | typing.Iterable[str] | ALL | None remove_classes: if specified, removes the given class(es)
            from the new element. The `ALL` sentinel value can be specified to remove all classes.
        :param bool copy_attrs: if True, copies the attributes of the source element to the new one. Defaults to True.
        :param bool copy_contents: if True, copies the contents of the source element to the new one. Defaults to True.
        :param dict[str, str] attributes: attributes to set on the new element, provided as keyword arguments.
            Will be str merged with the attributes of the source element, overriding the latter.
        :return: the new copied element.
        :rtype: etree.ElementBase
        """
        tag = tag or element.tag
        contents = innerxml(element, is_html=self.is_html) if copy_contents else None
        if copy_attrs:
            attributes = {**element.attrib, **attributes}
        new_element = self.build_element(tag, contents=contents, **attributes)
        edit_element_classes(new_element, add_classes, remove_classes, is_qweb=self.is_qweb)
        return new_element

    def adapt_xpath(self, xpath):
        """Adapts an xpath to match qweb ``t-att(f)-*`` attributes, if ``is_qweb`` is True."""
        return adapt_xpath_for_qweb(xpath) if self.is_qweb else xpath


def convert_tree(tree, src_version, dst_version, **converter_kwargs):
    """
    Convert an already parsed lxml tree from Bootstrap v3 to v4 inplace.

    :param etree.ElementTree tree: the lxml tree to convert.
    :param str src_version: the version of Bootstrap the document is currently using.
    :param str dst_version: the version of Bootstrap to convert the document to.
    :param dict converter_kwargs: additional keyword arguments to initialize :class:`~.BootstrapConverter`.
    :return: the converted lxml tree.
    :rtype: etree.ElementTree
    """
    tree, ops_count = BootstrapConverter(tree, **converter_kwargs).convert(src_version, dst_version)
    return tree


convert_arch = BootstrapConverter.convert_arch
convert_file = BootstrapConverter.convert_file


class BootstrapHTMLConverter:
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def __call__(self, content):
        if not content:
            return False, content
        converted_content = convert_arch(content, self.src, self.dst, is_html=True, is_qweb=True)
        return content != converted_content, converted_content
