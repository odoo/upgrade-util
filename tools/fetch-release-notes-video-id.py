#!/usr/bin/env -S uv run --script --quiet

# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx",
#   "libcst",
#   "lxml",
# ]
# ///

import re
import sys
from pathlib import Path

import httpx
import libcst as cst
from lxml import etree

if len(sys.argv) != 2:
    sys.exit(f"Usage: {sys.argv[0]} VERSION")

version = sys.argv[1]

VERSION_RE = re.compile(r"^(?:saas[~-])?([0-9]+)(?:\.([0-9]+))?$")

if (match := VERSION_RE.match(version)) is None:
    sys.exit(f"Invalid version: {version!r}")

major, minor = match.groups(default="0")

version_url = major if minor == "0" else f"{major}-{minor}"
full_version = f"{major}.0" if minor == "0" else f"saas~{major}.{minor}"

html = httpx.get(f"https://www.odoo.com/odoo-{version_url}-release-notes")
if html.status_code != 200:
    sys.exit(f"Cannot fetch release notes page for version {version}")

root = etree.fromstring(html.text, parser=etree.HTMLParser())
iframe = root.xpath("//main//iframe[contains(@src, 'youtube.com') or contains(@src, 'youtube-nocookie.com')]")
if not iframe:
    sys.exit(f"Cannot find youtube video in {html.url}")

yt_link = httpx.URL(iframe[0].attrib["src"])
video_id = yt_link.path.removeprefix("/embed/")


report_py = Path(__file__).parent.parent / "src" / "util" / "report.py"

source_tree = cst.parse_module(report_py.read_bytes())


class Transformer(cst.CSTTransformer):
    def __init__(self):
        self.video_dict = None
        self.key_found = False
        super().__init__()

    def visit_Assign(self, node):
        match node:
            case cst.Assign(
                targets=[cst.AssignTarget(target=cst.Name(value="ODOO_SHOWCASE_VIDEOS"))],
                value=video_dict,
            ):
                self.video_dict = video_dict
                return True
        return False

    def visit_Dict(self, node):
        return node is self.video_dict

    def leave_DictElement(self, original_node, updated_node):
        if original_node.key.raw_value == full_version:
            self.key_found = True
            if original_node.value.raw_value != video_id:
                updated_node = updated_node.with_changes(value=cst.SimpleString(f'"{video_id}"'))
        return updated_node

    def leave_Dict(self, original_node, updated_node):
        if original_node is self.video_dict:
            if self.key_found:
                elements = updated_node.elements
            else:
                new_elem = updated_node.elements[0].with_changes(
                    key=cst.SimpleString(f'"{full_version}"'), value=cst.SimpleString(f'"{video_id}"')
                )
                elements = [new_elem, *updated_node.elements]

            elements = sorted(elements, reverse=True, key=lambda e: VERSION_RE.match(e.key.raw_value).groups("0"))
            updated_node = updated_node.with_changes(elements=elements)
        return updated_node


modified_tree = source_tree.visit(Transformer())

report_py.write_text(modified_tree.code)
