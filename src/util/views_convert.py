"""Helpers to manipulate views/templates."""

import logging
import os.path
import re
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Iterable

from lxml import etree

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


class EtreeConverter(ABC):
    """
    Class for converting lxml etree documents, applying a bunch of operations on them.

    :param etree.ElementTree tree: the parsed XML or HTML tree to convert.
    :param bool is_html: whether the tree is an HTML document.
    :para bool is_qweb: whether the tree contains QWeb directives.
        If this is enabled, XPaths will be auto-transformed to try to also match ``t-att*`` attributes.
    """

    def __init__(self, tree, is_html=False, is_qweb=False):
        self.tree = tree
        self.is_html = is_html
        self.is_qweb = is_qweb

    @classmethod
    @abstractmethod
    def get_conversions(cls, *args, **kwargs):
        """Return the conversions to apply to the tree."""

    @classmethod
    @lru_cache(maxsize=32)
    def _compile_conversions(cls, conversions, is_qweb):
        """
        Compile the given conversions to a list of ``(xpath, operations)`` tuples, with pre-compiled XPaths.

        The conversions must be provided as tuples instead of lists to allow for caching.

        :param tuple[ElementOperation | (str, ElementOperation | tuple[ElementOperation, ...]), ...] conversions:
            the conversions to compile.
        :param bool is_qweb: whether the conversions are for QWeb.
        :rtype: list[(str, list[ElementOperation])]
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
    def prepare_conversions(cls, conversions, is_qweb):
        """Prepare and compile the conversions into a list of ``(xpath, operations)`` tuples, with pre-compiled XPaths."""
        # make sure conversions list and nested lists of operations are converted to tuples for caching
        conversions = tuple(
            (spec[0], tuple(spec[1]))
            if isinstance(spec, tuple) and len(spec) == 2 and isinstance(spec[1], list)
            else spec
            for spec in conversions
        )
        return cls._compile_conversions(conversions, is_qweb)

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
