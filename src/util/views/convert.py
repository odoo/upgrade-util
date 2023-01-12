"""Helpers to manipulate views/templates."""

import logging
import os.path
import re
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Iterable

from lxml import etree

from odoo.modules.module import get_modules

from .. import misc, pg, snippets
from . import records

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


def parse_arch(arch, is_html=False):
    """
    Parse a string of XML or HTML into a lxml :class:`etree.ElementTree`.

    :param str arch: the XML or HTML code to convert.
    :param bool is_html: whether the code is HTML or XML.
    :return: the parsed etree and the original document header (if any, removed from the tree).
    :rtype: (etree.ElementTree, str)

    :meta private: exclude from online docs
    """
    stripped_arch = arch.strip()
    doc_header_match = re.search(r"^<\?xml .+\?>\s*", stripped_arch)
    doc_header = doc_header_match.group(0) if doc_header_match else ""
    stripped_arch = stripped_arch[doc_header_match.end() :] if doc_header_match else stripped_arch

    return etree.fromstring(f"<wrap>{stripped_arch}</wrap>", parser=html_utf8_parser if is_html else None), doc_header


def unparse_arch(tree, doc_header="", is_html=False):
    """
    Convert an etree into a string of XML or HTML.

    :param etree.ElementTree tree: the etree to convert.
    :param str doc_header: the document header (if any).
    :param bool is_html: whether the code is HTML or XML.
    :return: the XML or HTML code.
    :rtype: str

    :meta private: exclude from online docs
    """
    wrap_node = tree.xpath("//wrap")[0]
    return doc_header + "\n".join(
        etree.tostring(child, encoding="unicode", with_tail=True, method="html" if is_html else None)
        for child in wrap_node
    )


class ArchEditor:
    """
    Context manager to edit an XML or HTML string.

    It will parse an XML or HTML string into an etree, and return it to its original
    string representation when exiting the context.

    The etree is available as the ``tree`` attribute of the context manager.
    The arch is available as the ``arch`` attribute of the context manager,
    and is updated when exiting the context.

    :param str arch: the XML or HTML code to convert.
    :param bool is_html: whether the code is HTML or XML.

    :meta private: exclude from online docs
    """

    def __init__(self, arch, is_html=False):
        self.arch = arch
        self.is_html = is_html
        self.doc_header = ""
        self.tree = None

    def __enter__(self):
        self.tree, self.doc_header = parse_arch(self.arch, is_html=self.is_html)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return
        self.arch = unparse_arch(self.tree, self.doc_header, is_html=self.is_html)

    def __str__(self):
        return self.arch


def innerxml(element, is_html=False):
    """
    Return the inner XML of an element as a string.

    :param etree.ElementBase element: the element to convert.
    :param bool is_html: whether to use HTML for serialization, XML otherwise. Defaults to False.
    :rtype: str

    :meta private: exclude from online docs
    """
    return (element.text or "") + "".join(
        etree.tostring(child, encoding=str, method="html" if is_html else None) for child in element
    )


def split_classes(*joined_classes):
    """
    Return a list of classes given one or more strings of joined classes separated by spaces.

    :meta private: exclude from online docs
    """
    return [c for classes in joined_classes for c in (classes or "").split(" ") if c]


def get_classes(element):
    """
    Return the list of classes from the ``class`` attribute of an element.

    :meta private: exclude from online docs
    """
    return split_classes(element.get("class", ""))


def join_classes(classes):
    """
    Return a string of classes joined by space given a list of classes.

    :meta private: exclude from online docs
    """
    return " ".join(classes)


def set_classes(element, classes):
    """
    Set the ``class`` attribute of an element from a list of classes.

    If the list is empty, the attribute is removed.

    :meta private: exclude from online docs
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

    :meta private: exclude from online docs
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

    :meta private: exclude from online docs
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

    :meta private: exclude from online docs
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

    :meta private: exclude from online docs
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

    :meta private: exclude from online docs
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
    """
    Adapts a xpath to enable matching on qweb ``t-att(f)-`` attributes.

    :meta private: exclude from online docs
    """
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


@lru_cache(maxsize=128)
def extract_xpath_keywords(xpath):
    """
    Extract keywords from an XPath expression.

    Will return a tuple of sets for class names and other keywords (e.g. tags, attrs).

    :param str xpath: the XPath expression.
    :rtype: (frozenset[str], frozenset[str])

    :meta private: exclude from online docs
    """
    kwd_pattern = r"[A-Za-z][\w-]*"

    classes = set()
    other_kwds = set()

    class_match = re.search(rf"hasclass\s*\(\s*['\"]({kwd_pattern})['\"]\s*\)", xpath)
    if class_match:
        classes.add(class_match.group(1))

    regex_match = re.search(r"regex\((.*)\)", xpath)
    if regex_match:
        regex_inner = regex_match.group(1)
        matches = re.findall(rf"(?<![(|@\[-\\])\b{kwd_pattern}", regex_inner)
        classes.update(matches)

    if not class_match and not regex_match:
        attr_match = re.search(rf"@({kwd_pattern})\b", xpath)
        if attr_match:
            other_kwds.add(attr_match.group(1))

        tag_match = re.search(rf"(?<=/)({kwd_pattern})\b(?!\s*\()", xpath)
        if tag_match:
            other_kwds.add(tag_match.group(1))

    other_kwds -= {"class", "regex", "concat", "hasclass"}

    return frozenset(classes), frozenset(other_kwds)


def extract_xpaths_keywords(xpaths):
    """
    Build and return a list of keywords from the given xpath expressions.

    Same as :func:`extract_path_keywords` but over multiple xpath expressions.
    Will return a tuple of sets for class names and other keywords (e.g. tags, attrs)
    collected for all the given xpath expressions.

    :param typing.Iterable[str | etree.XPath] xpaths: the XPath expressions.
    :rtype: (set[str], set[str])

    :meta private: exclude from online docs
    """
    classes = set()
    other_kwds = set()
    for xpath in xpaths:
        if isinstance(xpath, etree.XPath):
            xpath = xpath.path  # noqa: PLW2901
        if not isinstance(xpath, str):
            raise TypeError(f"Expected str or etree.XPath, got {type(xpath).__name__}: {xpath!r}")
        xpath_classes, xpath_other_kwds = extract_xpath_keywords(xpath)
        classes |= xpath_classes
        other_kwds |= xpath_other_kwds

    return classes, other_kwds


@lru_cache(maxsize=128)
def build_keywords_where_clause(cr, classes, other_kwds, column):
    """
    Build a WHERE clause to filter records based on keywords.

    N.B. arguments must be hashable to enable caching.

    :param psycopg2.cursor cr: the database cursor.
    :param tuple[str] classes: elements class names to match.
    :param tuple[str] other_kwds: other keywords to match.
    :param str column: the column in the query to match the keywords against.
    :rtype: str

    :meta private: exclude from online docs
    """

    def add_word_delimiters(keyword):
        keyword = re.sub(r"^(?=\w)", r"\\m", keyword)
        return re.sub(r"(?<=\w)$", r"\\M", keyword)

    column = ".".join(f'"{part}"' for part in column.split("."))
    return cr.mogrify(
        rf"(({column} ~ '\mclass\M\s*=' AND {column} ~ %(classes)s) OR {column} ~ %(others)s)",
        {
            "classes": "|".join(add_word_delimiters(cls) for cls in classes),
            "others": "|".join(add_word_delimiters(kw) for kw in other_kwds),
        },
    ).decode()


def build_xpaths_where_clause(cr, xpaths, column):
    """
    Build a WHERE clause to filter records based on XPath expressions.

    :param psycopg2.cursor cr: the database cursor.
    :param typing.Iterable[str | etree.XPath] xpaths: the XPath expressions to match.
    :param str column: the column in the query to match the XPath expressions against.

    :meta private: exclude from online docs
    """
    classes, other_kwds = extract_xpaths_keywords(xpaths)
    return build_keywords_where_clause(cr, tuple(classes), tuple(other_kwds), column)


class ElementOperation(ABC):
    """
    Abstract base class for operations that can be applied on xml/html elements.

    The concrete subclasses can then be used to define operations for fixing/converting views.

    :param str | None xpath: an optional xpath to match on that will be included in the operation definition.
        If None or omitted it must be provided separately to the converter.

    :meta private: exclude from online docs
    """

    def __init__(self, *, xpath=None):
        self._xpath = xpath

    @abstractmethod
    def __call__(self, element, converter):
        """
        Perform the operation on the given element.

        Abstract method that must be implemented by subclasses.

        :param etree.ElementBase element: the etree element to apply the operation on.
        :param BootstrapConverter converter: the converter that's operating on the etree document.
        :return: the converted element, which could be the same provided, a different one, or None.
            The returned element should be used by the converter to chain further operations.
        :rtype: etree.ElementBase | None

        :meta private: exclude from online docs
        """

    @property
    def xpath(self):
        """
        The XPath expression that matches elements for the defined operation.

        It will be used by the converter to match elements and apply the operation on them.

        :raise ValueError: if the operation does not define an XPath.
        :rtype: str

        :meta private: exclude from online docs
        """
        if self._xpath is None:
            raise ValueError(
                f"Operation {self.__class__.__name__} does not provide a default XPath "
                "for matching elements and none was provided either at definition time, "
                "or within the conversions list."
            )
        return self._xpath


class RemoveElement(ElementOperation):
    """
    Remove the matched element(s) from the document.

    :meta private: exclude from online docs
    """

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
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, *, add=None, remove=None, xpath=None):
        super().__init__(xpath=xpath)

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

    @property
    def xpath(self):
        if not self.remove_classes:
            raise ValueError("Cannot generate an XPath expression without any classes to remove (i.e. match on)")
        xpath_pre = self._xpath or "//*"
        if self.remove_classes is ALL:
            if not self._xpath:
                raise ValueError(
                    f"{self.__class__.__name__}: Attempted to generate XPath matching any class for any element. "
                    "Provide an additional `xpath=` argument to narrow down the elements to match."
                )
            return xpath_pre + "[@class]"
        return xpath_pre + ("[" + " or ".join(f"hasclass('{c}')" for c in self.remove_classes) + "]")


class AddClasses(EditClasses):
    """
    Add classes.

    :param str | typing.Iterable[str] classes: the classes to add to the elements.
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, *classes, xpath=None):
        super().__init__(add=classes, xpath=xpath)

    xpath = ElementOperation.xpath  # skip EditClasses xpath method logic


class RemoveClasses(EditClasses):
    """
    Remove classes.

    N.B. no checks are made to ensure the class(es) to remove are actually present on the elements.

    :param str | typing.Iterable[str] | ALL classes: the classes to remove from the elements.
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, *classes, xpath=None):
        super().__init__(remove=classes, xpath=xpath)


class ReplaceClasses(EditClasses):
    """
    Replace old classes with new ones.

    :param str | typing.Iterable[str] | ALL old: classes to remove from the elements.
    :param str | typing.Iterable[str] | None new: classes to add to the elements.
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, old, new, *, xpath=None):
        if not old:
            raise ValueError("At least one class to remove must be specified")
        super().__init__(remove=old, add=new, xpath=xpath)


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

    :meta private: exclude from online docs
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
    """
    Rename an attribute. Silently ignores elements that do not have the attribute.

    :param str old_name: the name of the attribute to rename.
    :param str new_name: the new name of the attribute.
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, old_name, new_name, *, xpath=None):
        super().__init__(xpath=xpath)
        self.old_name = old_name
        self.new_name = new_name

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

    @property
    def xpath(self):
        """
        Return an XPath expression that matches elements with the old attribute name.

        :rtype: str

        :meta private: exclude from online docs
        """
        return (self._xpath or "//*") + f"[@{self.old_name}]"


class RegexReplace(ElementOperation):
    """
    Uses `re.sub` to modify an attribute or the text of an element.

    N.B. no checks are made to ensure the attribute to replace is actually present on the elements.

    :param str pattern: the regex pattern to match.
    :param str sub: the replacement string.
    :param str | None attr: the attribute to replace. If not specified, the text of the element is replaced.
    :param str | None xpath: see :class:`ElementOperation` ``xpath`` parameter.

    :meta private: exclude from online docs
    """

    def __init__(self, pattern, sub, attr=None, *, xpath=None):
        super().__init__(xpath=xpath)
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

    @property
    def xpath(self):
        """
        Return an XPath expression that matches elements with the old attribute name.

        :rtype: str

        :meta private: exclude from online docs
        """
        return regex_xpath(self.pattern, self.attr, self._xpath)


class RegexReplaceClass(RegexReplace):
    """
    Uses `re.sub` to modify the class.

    Basically, same as `RegexReplace`, but with `attr="class"`.

    :meta private: exclude from online docs
    """

    def __init__(self, pattern, sub, attr="class", *, xpath=None):
        super().__init__(pattern, sub, attr, xpath=xpath)


class EtreeConverter:
    """
    Class for converting lxml etree documents, applying a bunch of operations on them.

    :param list[ElementOperation | (str, ElementOperation | list[ElementOperation])] conversions:
        the operations to apply to the tree.
        Each item in the conversions list must either be an :class:`ElementOperation` that can provide its own XPath,
        or a tuple of ``(xpath, operation)`` or ``(xpath, operations)`` with the XPath and an operation
        or a list of operations to apply to the nodes matching the XPath.
    :param bool is_html: whether the tree is an HTML document.
    :para bool is_qweb: whether the tree contains QWeb directives.
        If this is enabled, XPaths will be auto-transformed to try to also match ``t-att*`` attributes.

    :meta private: exclude from online docs
    """

    def __init__(self, conversions, *, is_html=False, is_qweb=False):
        self._hashable_conversions = self._make_conversions_hashable(conversions)
        conversions_hash = hash(self._hashable_conversions)
        self._is_html = is_html
        self._is_qweb = is_qweb
        self.conversions = self._compile_conversions(self._hashable_conversions, self.is_qweb)
        self._cache_hash = hash((self.__class__, conversions_hash, is_html, is_qweb))

    def __hash__(self):
        return self._cache_hash

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["conversions"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.conversions = self._compile_conversions(self._hashable_conversions, self.is_qweb)

    @property
    def is_html(self):
        """
        Whether the conversions are for HTML documents.

        :meta private: exclude from online docs
        """
        return self._is_html

    @property
    def is_qweb(self):
        """
        Whether the conversions are for QWeb documents.

        :meta private: exclude from online docs
        """
        return self._is_qweb

    @classmethod
    @lru_cache(maxsize=32)
    def _compile_conversions(cls, conversions, is_qweb):
        """
        Compile the given conversions to a list of ``(xpath, operations)`` tuples, with pre-compiled XPaths.

        The conversions must be provided as tuples instead of lists to allow for caching.

        :param tuple[ElementOperation | (str, ElementOperation | tuple[ElementOperation, ...]), ...] conversions:
            the conversions to compile.
        :param bool is_qweb: whether the conversions are for QWeb.
        :rtype: list[(etree.XPath, list[ElementOperation])]
        """

        def process_spec(spec):
            xpath, ops = None, None
            if isinstance(spec, ElementOperation):  # single operation with its own XPath
                xpath, ops = spec.xpath, [spec]
            elif isinstance(spec, tuple) and len(spec) == 2:  # (xpath, operation | operations) tuple
                xpath, ops_spec = spec
                if isinstance(ops_spec, ElementOperation):  # single operation
                    ops = [ops_spec]
                elif isinstance(ops_spec, tuple):  # multiple operations
                    ops = list(ops_spec)

            if xpath is None or ops is None:
                raise ValueError(f"Invalid conversion specification: {spec!r}")

            if is_qweb:
                xpath = adapt_xpath_for_qweb(xpath)

            return etree.XPath(xpath), ops

        return [process_spec(spec) for spec in conversions]

    @classmethod
    def _make_conversions_hashable(cls, conversions):
        """
        Normalize the given conversions into tuples, so they can be hashed.

        :param list[ElementOperation | (str, ElementOperation | list[ElementOperation])] conversions:
            the conversions to make hashable.
        :rtype: tuple[ElementOperation | (str, ElementOperation | tuple[ElementOperation, ...]), ...]
        """
        return tuple(
            (spec[0], tuple(spec[1]))
            if isinstance(spec, tuple) and len(spec) == 2 and isinstance(spec[1], list)
            else spec
            for spec in conversions
        )

    def get_conversions_keywords(self):
        """
        Build and return keywords extracted from the compiled conversions.

        Will return a tuple of sets for class names and other keywords (e.g. tags, attrs).

        :rtype: (set[str], set[str])

        :meta private: exclude from online docs
        """
        return extract_xpaths_keywords(xpath for xpath, _ in self.conversions)

    def build_where_clause(self, cr, column):
        """
        Build and return an XPath expression that matches all the conversions keywords.

        :param psycopg2.cursor cr: the database cursor.
        :param str column: the column in the query to match the keywords against.
        :rtype: str

        :meta private: exclude from online docs
        """
        classes, other_kwds = self.get_conversions_keywords()
        return build_keywords_where_clause(cr, tuple(classes), tuple(other_kwds), column)

    def convert_tree(self, tree):
        """
        Convert an etree document inplace with the prepared conversions.

        Returns the converted document and the number of conversion operations applied.

        :param etree.ElementTree tree: the parsed XML or HTML tree to convert.
        :rtype: etree.ElementTree, int

        :meta private: exclude from online docs
        """
        applied_operations_count = 0
        for xpath, operations in self.conversions:
            for element in xpath(tree):
                for operation in operations:
                    if element is None:  # previous operations that returned None (i.e. deleted element)
                        raise ValueError("Matched xml element is not available anymore! Check operations.")
                    element = operation(element, self)  # noqa: PLW2901
                    applied_operations_count += 1
        return tree, applied_operations_count

    convert = convert_tree  # alias for backward compatibility

    @lru_cache(maxsize=128)  # noqa: B019
    def convert_callback(self, content):
        """
        A converter method that can be used with ``util.snippets`` ``convert_html_columns`` or ``convert_html_content``.

        Accepts a single argument, the html/xml content to convert, and returns a tuple of (has_changed, converted_content).

        :param str content: the html/xml content to convert.
        :rtype: (bool, str)

        :meta private: exclude from online docs
        """  # noqa: D401
        if not content:
            return False, content

        with ArchEditor(content, self.is_html) as arch_editor:
            tree, ops_count = self.convert_tree(arch_editor.tree)
            if not ops_count:
                return False, content
        return True, arch_editor.arch

    def convert_arch(self, arch):
        """
        Convert an XML or HTML arch string with the prepared conversions.

        :param str arch: the arch to convert.
        :rtype: str

        :meta private: exclude from online docs
        """
        return self.convert_callback(arch)[1]

    def convert_file(self, path):
        """
        Convert an XML or HTML file inplace.

        :param str path: the path to the XML or HTML file to convert.
        :rtype: None

        :meta private: exclude from online docs
        """
        file_is_html = os.path.splitext(path)[1].startswith("htm")
        if self.is_html != file_is_html:
            raise ValueError(f"File {path!r} is not a {'HTML' if self.is_html else 'XML'} file!")

        tree = etree.parse(path, parser=html_utf8_parser if self.is_html else None)

        tree, ops_count = self.convert_tree(tree)
        if not ops_count:
            logging.info("No conversion operations applied, skipping file: %s", path)
            return

        tree.write(path, encoding="utf-8", method="html" if self.is_html else None, xml_declaration=not self.is_html)

    # -- Operations helper methods, useful where operations need some converter-specific info or logic (e.g. is_html) --

    def element_factory(self, *args, **kwargs):
        """
        Create new elements using the correct document type.

        Basically a wrapper for either etree.XML or etree.HTML depending on the type of document loaded.

        :param args: positional arguments to pass to the etree.XML or etree.HTML function.
        :param kwargs: keyword arguments to pass to the etree.XML or etree.HTML function.
        :return: the created element.

        :meta private: exclude from online docs
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

        :meta private: exclude from online docs
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

        :meta private: exclude from online docs
        """
        tag = tag or element.tag
        contents = innerxml(element, is_html=self.is_html) if copy_contents else None
        if copy_attrs:
            attributes = {**element.attrib, **attributes}
        new_element = self.build_element(tag, contents=contents, **attributes)
        edit_element_classes(new_element, add_classes, remove_classes, is_qweb=self.is_qweb)
        return new_element

    def adapt_xpath(self, xpath):
        """
        Adapts an XPath to match qweb ``t-att(f)-*`` attributes, if ``is_qweb`` is True.

        :meta private: exclude from online docs
        """
        return adapt_xpath_for_qweb(xpath) if self.is_qweb else xpath


if misc.version_gte("13.0"):

    def convert_views(cr, views_ids, converter):
        """
        Convert the specified views xml arch using the provided converter.

        :param psycopg2.cursor cr: the database cursor.
        :param typing.Collection[int] views_ids: the ids of the views to convert.
        :param EtreeConverter converter: the converter to use.
        :rtype: None

        :meta private: exclude from online docs
        """
        if converter.is_html:
            raise TypeError("Cannot convert xml views with provided ``is_html`` converter %s" % (repr(converter),))

        _logger.info("Converting %d views/templates using %s", len(views_ids), repr(converter))
        for view_id in views_ids:
            with records.edit_view(cr, view_id=view_id, active=None) as tree:
                converter.convert_tree(tree)
            # TODO abt: maybe notify in the log or report that custom views with noupdate=False were converted?

    def convert_qweb_views(cr, converter):
        """
        Convert QWeb views / templates using the provided converter.

        :param psycopg2.cursor cr: the database cursor.
        :param EtreeConverter converter: the converter to use.
        :rtype: None

        :meta private: exclude from online docs
        """
        if not converter.is_qweb:
            raise TypeError("Converter for xml views must be ``is_qweb``, got %s" % (repr(converter),))

        # views to convert must have `website_id` set and not come from standard modules
        standard_modules = set(get_modules()) - {"studio_customization", "__export__", "__cloc_exclude__"}
        converter_where = converter.build_where_clause(cr, "v.arch_db")

        # Search for custom/cow'ed views (they have no external ID)... but also
        # search for views with external ID that have a related COW'ed view. Indeed,
        # when updating a generic view after this script, the archs are compared to
        # know if the related COW'ed views must be updated too or not: if we only
        # convert COW'ed views they won't get the generic view update as they will be
        # judged different from them (user customization) because of the changes
        # that were made.
        # E.g.
        # - In 15.0, install website_sale
        # - Enable eCommerce categories: a COW'ed view is created to enable the
        #   feature (it leaves the generic disabled and creates an exact copy but
        #   enabled)
        # - Migrate to 16.0: you expect your enabled COW'ed view to get the new 16.0
        #   version of eCommerce categories... but if the COW'ed view was converted
        #   while the generic was not, they won't be considered the same
        #   anymore and only the generic view will get the 16.0 update.
        cr.execute(
            """
            WITH keys AS (
                  SELECT key
                    FROM ir_ui_view
                GROUP BY key
                  HAVING COUNT(*) > 1
            )
               SELECT v.id
                 FROM ir_ui_view v
            LEFT JOIN ir_model_data imd
                   ON imd.model = 'ir.ui.view'
                  AND imd.module IN %%s
                  AND imd.res_id = v.id
            LEFT JOIN keys
                   ON v.key = keys.key
                WHERE v.type = 'qweb'
                  AND (%s)
                  AND (
                      imd.id IS NULL
                      OR (
                          keys.key IS NOT NULL
                          AND imd.noupdate = FALSE
                      )
                  )
            """
            % converter_where,
            [tuple(standard_modules)],
        )
        views_ids = [view_id for (view_id,) in cr.fetchall()]
        if views_ids:
            convert_views(cr, views_ids, converter)

    def convert_html_fields(cr, converter):
        """
        Convert all html fields data in the database using the provided converter.

        :param psycopg2.cursor cr: the database cursor.
        :param EtreeConverter converter: the converter to use.
        :rtype: None

        :meta private: exclude from online docs
        """
        _logger.info("Converting html fields data using %s", repr(converter))

        html_fields = list(snippets.html_fields(cr))
        for table, columns in misc.log_progress(html_fields, _logger, "tables", log_hundred_percent=True):
            if table not in ("mail_message", "mail_activity"):
                extra_where = " OR ".join(
                    "(%s)" % converter.build_where_clause(cr, pg.get_value_or_en_translation(cr, table, column))
                    for column in columns
                )
            snippets.convert_html_columns(cr, table, columns, converter.convert_callback, extra_where=extra_where)

else:

    def convert_views(*args, **kwargs):
        raise NotImplementedError(
            "This helper function is only available for Odoo 13.0 and above: %s", convert_views.__qualname__
        )

    def convert_qweb_views(*args, **kwargs):
        raise NotImplementedError(
            "This helper function is only available for Odoo 13.0 and above: %s", convert_qweb_views.__qualname__
        )

    def convert_html_fields(*args, **kwargs):
        raise NotImplementedError(
            "This helper function is only available for Odoo 13.0 and above: %s", convert_html_fields.__qualname__
        )
