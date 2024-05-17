"""Convert an XML/HTML document Bootstrap code from an older version to a newer one."""

import logging
from functools import lru_cache

from lxml import etree

try:
    from packaging.version import Version
except ImportError:
    from distutils.version import StrictVersion as Version  # N.B. deprecated, will be removed in py3.12

from .convert import (
    ALL,
    BE,
    BS,
    CSS,
    AddClasses,
    ElementOperation,
    EtreeConverter,
    PullUp,
    RegexReplaceClass,
    RemoveClasses,
    RemoveElement,
    RenameAttribute,
    ReplaceClasses,
    edit_element_classes,
    get_classes,
    regex_xpath,
)

_logger = logging.getLogger(__name__)


class BS3to4ConvertBlockquote(ElementOperation):
    """
    Convert a BS3 ``<blockquote>`` element to a BS4 ``<div>`` element with the ``blockquote`` class.

    :meta private: exclude from online docs
    """

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

    :meta private: exclude from online docs
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
    """
    Fully convert a BS3 panel, thumbnail, or well element and their contents to a BS4 card.

    :meta private: exclude from online docs
    """

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

    :meta private: exclude from online docs
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
    """
    Convert BS4 ``.card-deck`` elements to grid components (``.row``, ``.col``, etc.).

    :meta private: exclude from online docs
    """

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
    """
    Convert BS4 ``.form-inline`` elements to grid components (``.row``, ``.col``, etc.).

    :meta private: exclude from online docs
    """

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


class BootstrapConverter(EtreeConverter):
    """
    Class for converting XML or HTML Bootstrap code across versions.

    :param str src_version: the source Bootstrap version to convert from.
    :param str dst_version: the destination Bootstrap version to convert to.
    :param bool is_html: whether the tree is an HTML document.
    :param bool is_qweb: whether the tree contains QWeb directives. See :class:`EtreeConverter` for more info.

    :meta private: exclude from online docs
    """

    MIN_VERSION = "3.0"
    """Minimum supported Bootstrap version."""

    # Conversions definitions by destination version.
    # It's a dictionary of version strings to a conversions list.
    # Each item in the conversions list must either be an :class:`ElementOperation`,
    # that can provide its own XPath, or a tuple of ``(xpath, operation(s))`` with the XPath
    # and a single operation or a list of operations to apply to the nodes matching the XPath.
    CONVERSIONS = {
        "4.0": [
            # inputs
            (CSS(".form-group .control-label"), ReplaceClasses("control-label", "form-control-label")),
            (CSS(".form-group .text-help"), ReplaceClasses("text-help", "form-control-feedback")),
            (CSS(".control-group .help-block"), ReplaceClasses("help-block", "form-text")),
            ReplaceClasses("form-group-sm", "form-control-sm"),
            ReplaceClasses("form-group-lg", "form-control-lg"),
            (CSS(".form-control .input-sm"), ReplaceClasses("input-sm", "form-control-sm")),
            (CSS(".form-control .input-lg"), ReplaceClasses("input-lg", "form-control-lg")),
            # hide
            ReplaceClasses("hidden-xs", "d-none"),
            ReplaceClasses("hidden-sm", "d-sm-none"),
            ReplaceClasses("hidden-md", "d-md-none"),
            ReplaceClasses("hidden-lg", "d-lg-none"),
            ReplaceClasses("visible-xs", "d-block d-sm-none"),
            ReplaceClasses("visible-sm", "d-block d-md-none"),
            ReplaceClasses("visible-md", "d-block d-lg-none"),
            ReplaceClasses("visible-lg", "d-block d-xl-none"),
            # image
            ReplaceClasses("img-rounded", "rounded"),
            ReplaceClasses("img-circle", "rounded-circle"),
            ReplaceClasses("img-responsive", ("d-block", "img-fluid")),
            # buttons
            ReplaceClasses("btn-default", "btn-secondary"),
            ReplaceClasses("btn-xs", "btn-sm"),
            (CSS(".btn-group.btn-group-xs"), ReplaceClasses("btn-group-xs", "btn-group-sm")),
            (CSS(".dropdown .divider"), ReplaceClasses("divider", "dropdown-divider")),
            ReplaceClasses("badge", "badge badge-pill"),
            ReplaceClasses("label", "badge"),
            RegexReplaceClass(rf"{BS}label-(default|primary|success|info|warning|danger){BE}", r"badge-\1"),
            (CSS(".breadcrumb > li"), ReplaceClasses("breadcrumb", "breadcrumb-item")),
            # li
            (CSS(".list-inline > li"), AddClasses("list-inline-item")),
            # pagination
            (CSS(".pagination > li"), AddClasses("page-item")),
            (CSS(".pagination > li > a"), AddClasses("page-link")),
            # carousel
            (CSS(".carousel .carousel-inner > .item"), ReplaceClasses("item", "carousel-item")),
            # pull
            ReplaceClasses("pull-right", "float-right"),
            ReplaceClasses("pull-left", "float-left"),
            ReplaceClasses("center-block", "mx-auto"),
            # well
            (CSS(".well"), BS3to4MakeCard()),
            (CSS(".thumbnail"), BS3to4MakeCard()),
            # blockquote
            (CSS("blockquote"), BS3to4ConvertBlockquote()),
            (CSS(".blockquote.blockquote-reverse"), ReplaceClasses("blockquote-reverse", "text-right")),
            # dropdown
            (CSS(".dropdown-menu > li > a"), AddClasses("dropdown-item")),
            (CSS(".dropdown-menu > li"), PullUp()),
            # in
            ReplaceClasses("in", "show"),
            # table
            (CSS("tr.active, td.active"), ReplaceClasses("active", "table-active")),
            (CSS("tr.success, td.success"), ReplaceClasses("success", "table-success")),
            (CSS("tr.info, td.info"), ReplaceClasses("info", "table-info")),
            (CSS("tr.warning, td.warning"), ReplaceClasses("warning", "table-warning")),
            (CSS("tr.danger, td.danger"), ReplaceClasses("danger", "table-danger")),
            (CSS("table.table-condesed"), ReplaceClasses("table-condesed", "table-sm")),
            # navbar
            (CSS(".nav.navbar > li > a"), AddClasses("nav-link")),
            (CSS(".nav.navbar > li"), AddClasses("nav-intem")),
            ReplaceClasses("navbar-btn", "nav-item"),
            (CSS(".navbar-nav"), ReplaceClasses("navbar-right nav", "ml-auto")),
            ReplaceClasses("navbar-toggler-right", "ml-auto"),
            (CSS(".navbar-nav > li > a"), AddClasses("nav-link")),
            (CSS(".navbar-nav > li"), AddClasses("nav-item")),
            (CSS(".navbar-nav > a"), AddClasses("navbar-brand")),
            ReplaceClasses("navbar-fixed-top", "fixed-top"),
            ReplaceClasses("navbar-toggle", "navbar-toggler"),
            ReplaceClasses("nav-stacked", "flex-column"),
            (CSS("nav.navbar"), AddClasses("navbar-expand-lg")),
            (CSS("button.navbar-toggle"), ReplaceClasses("navbar-toggle", "navbar-expand-md")),
            # card
            (CSS(".panel"), BS3to4ConvertCard()),
            (CSS(".card"), BS3to4ConvertCard()),
            # grid
            RegexReplaceClass(rf"{BS}col((?:-\w{{2}})?)-offset-(\d{{1,2}}){BE}", r"offset\1-\2"),
        ],
        "5.0": [
            # links
            RegexReplaceClass(rf"{BS}text-(?!o-)", "link-", xpath=CSS("a")),
            (CSS(".nav-item.active > .nav-link"), AddClasses("active")),
            (CSS(".nav-link.active") + CSS(".nav-item.active", prefix="/parent::"), RemoveClasses("active")),
            # badges
            ReplaceClasses("badge-pill", "rounded-pill"),
            RegexReplaceClass(rf"{BS}badge-", r"text-bg-"),
            # buttons
            ("//*[hasclass('btn-block')]/parent::div", AddClasses("d-grid gap-2")),
            ("//*[hasclass('btn-block')]/parent::p[count(./*)=1]", AddClasses("d-grid gap-2")),
            RemoveClasses("btn-block"),
            (CSS("button.close"), BS4to5ConvertCloseButton()),
            # card
            # TODO abt: .card-columns (unused in odoo)
            (CSS(".card-deck"), BS4to5ConvertCardDeck()),
            # jumbotron
            ReplaceClasses("jumbotron", "container-fluid py-5"),
            # new data-bs- attributes
            RenameAttribute("data-display", "data-bs-display", xpath="//*[not(@data-snippet='s_countdown')]"),
            *[
                RenameAttribute(f"data-{attr}", f"data-bs-{attr}")
                for attr in (
                    "animation attributes autohide backdrop body container content delay dismiss focus"
                    " interval margin-right no-jquery offset original-title padding-right parent placement"
                    " ride sanitize show slide slide-to spy target toggle touch trigger whatever"
                ).split(" ")
            ],
            # popover
            (CSS(".popover .arrow"), ReplaceClasses("arrow", "popover-arrow")),
            # form
            ReplaceClasses("form-row", "row"),
            ("//*[hasclass('form-group')]/parent::form", AddClasses("row")),
            ReplaceClasses("form-group", "col-12 py-2"),
            (CSS(".form-inline"), BS4to5ConvertFormInline()),
            ReplaceClasses("custom-checkbox", "form-check"),
            RegexReplaceClass(rf"{BS}custom-control-(input|label){BE}", r"form-check-\1"),
            RegexReplaceClass(rf"{BS}custom-control-(input|label){BE}", r"form-check-\1"),
            RegexReplaceClass(rf"{BS}custom-(check|select|range){BE}", r"form-\1"),
            (CSS(".custom-switch"), ReplaceClasses("custom-switch", "form-check form-switch")),
            ReplaceClasses("custom-radio", "form-check"),
            RemoveClasses("custom-control"),
            (CSS(".custom-file"), PullUp()),
            RegexReplaceClass(rf"{BS}custom-file-", r"form-file-"),
            RegexReplaceClass(rf"{BS}form-file(?:-input)?{BE}", r"form-control"),
            (CSS("label.form-file-label"), RemoveElement()),
            (regex_xpath(rf"{BS}input-group-(prepend|append){BE}", "class"), PullUp()),
            ("//label[not(hasclass('form-check-label'))]", AddClasses("form-label")),
            ReplaceClasses("form-control-file", "form-control"),
            ReplaceClasses("form-control-range", "form-range"),
            # TODO abt: .form-text no loger sets display, add some class?
            # table
            RegexReplaceClass(rf"{BS}thead-(light|dark){BE}", r"table-\1"),
            # grid
            RegexReplaceClass(rf"{BS}col-((?:\w{{2}}-)?)offset-(\d{{1,2}}){BE}", r"offset-\1\2"),  # from BS4
            # gutters
            ReplaceClasses("no-gutters", "g-0"),
            # logical properties
            RegexReplaceClass(rf"{BS}left-((?:\w{{2,3}}-)?[0-9]+|auto){BE}", r"start-\1"),
            RegexReplaceClass(rf"{BS}right-((?:\w{{2,3}}-)?[0-9]+|auto){BE}", r"end-\1"),
            RegexReplaceClass(rf"{BS}((?:float|border|rounded|text)(?:-\w+)?)-left{BE}", r"\1-start"),
            RegexReplaceClass(rf"{BS}((?:float|border|rounded|text)(?:-\w+)?)-right{BE}", r"\1-end"),
            RegexReplaceClass(rf"{BS}rounded-sm(-(?:start|end|top|bottom))?", r"rounded\1-1"),
            RegexReplaceClass(rf"{BS}rounded-lg(-(?:start|end|top|bottom))?", r"rounded\1-3"),
            RegexReplaceClass(rf"{BS}([mp])l-((?:\w{{2,3}}-)?(?:[0-9]+|auto)){BE}", r"\1s-\2"),
            RegexReplaceClass(rf"{BS}([mp])r-((?:\w{{2,3}}-)?(?:[0-9]+|auto)){BE}", r"\1e-\2"),
            ReplaceClasses("dropdown-menu-left", "dropdown-menu-start"),
            ReplaceClasses("dropdown-menu-right", "dropdown-menu-end"),
            ReplaceClasses("dropleft", "dropstart"),
            ReplaceClasses("dropright", "dropend"),
            # tooltips
            (
                "//*[hasclass('tooltip') or @role='tooltip']//*[hasclass('arrow')]",
                ReplaceClasses("arrow", "tooltip-arrow"),
            ),
            # utilities
            ReplaceClasses("text-monospace", "font-monospace"),
            RegexReplaceClass(rf"{BS}font-weight-", r"fw-"),
            RegexReplaceClass(rf"{BS}font-style-", r"fst-"),
            ReplaceClasses("font-italic", "fst-italic"),
            # helpers
            RegexReplaceClass(rf"{BS}embed-responsive-(\d+)by(\d+)", r"ratio-\1x\2"),
            RegexReplaceClass(rf"{BS}ratio-(\d+)by(\d+)", r"ratio-\1x\2"),
            RegexReplaceClass(rf"{BS}embed-responsive(?!-)", r"ratio"),
            RegexReplaceClass(rf"{BS}sr-only(-focusable)?", r"visually-hidden\1"),
            # media
            ReplaceClasses("media-body", "flex-grow-1"),
            ReplaceClasses("media", "d-flex"),
        ],
    }

    def __init__(self, src_version, dst_version, *, is_html=False, is_qweb=False):
        self.src_version = src_version
        self.dst_version = dst_version
        conversions = self._get_conversions(src_version, dst_version)
        super().__init__(conversions, is_html=is_html, is_qweb=is_qweb)

    @classmethod
    def _get_sorted_conversions(cls):
        """
        Return the conversions dict sorted by version, from oldest to newest.

        :meta private: exclude from online docs
        """
        return sorted(cls.CONVERSIONS.items(), key=lambda kv: Version(kv[0]))

    @classmethod
    @lru_cache(maxsize=8)
    def _get_conversions(cls, src_version, dst_version):
        """
        Return the list of conversions to convert Bootstrap from ``src_version`` to ``dst_version``.

        :param str src_version: the source Bootstrap version.
        :param str dst_version: the destination Bootstrap version.
        :rtype: list[ElementOperation | (str, ElementOperation | list[ElementOperation])]

        :meta private: exclude from online docs
        """
        if Version(dst_version) < Version(src_version):
            raise NotImplementedError("Downgrading Bootstrap versions is not supported.")
        if Version(src_version) < Version(cls.MIN_VERSION):
            raise NotImplementedError(f"Conversion from Bootstrap version {src_version} is not supported")

        result = []
        for version, conversions in BootstrapConverter._get_sorted_conversions():
            if Version(src_version) < Version(version) <= Version(dst_version):
                result.extend(conversions)

        if not result:
            if Version(src_version) == Version(dst_version):
                _logger.info("Source and destination versions are the same, no conversion needed.")
            else:
                raise NotImplementedError(f"Conversion from {src_version} to {dst_version} is not supported")

        return result


# TODO abt: remove this / usages -> replace with refactored converter classes
class BootstrapHTMLConverter:
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def __call__(self, content):
        if not content:
            return False, content
        converted_content = BootstrapConverter.convert_arch(content, self.src, self.dst, is_html=True, is_qweb=True)
        return content != converted_content, converted_content
