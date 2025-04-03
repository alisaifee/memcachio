#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("../../"))
sys.path.insert(0, os.path.abspath("./"))

import requests
from theme_config import *

import memcachio
import memcachio.types

try:
    latest_version = requests.get("https://pypi.org/pypi/memcachio/json").json()["info"][
        "version"
    ]
except:
    latest_version = None

master_doc = "index"
project = "memcachio"
copyright = "2025, Ali-Akber Saifee"
author = "alisaifee"
description = "Async memcached client for python"

html_static_path = ["./_static"]
html_css_files = [
    "custom.css",
    "https://fonts.googleapis.com/css2?family=Fira+Code:wght@300;400;700&family=Fira+Sans:ital,wght@0,100;0,200;0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,100;1,200;1,300;1,400;1,500;1,600;1,700;1,800;1,900&display=swap",
]

html_baseurl = "https://memcachio.readthedocs.io/"
sitemap_url_scheme = "en/stable/{link}"

extensions = [
    "enum_tools.autoenum",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_issues",
    "sphinx_sitemap",
    "sphinxcontrib.programoutput",
    "sphinxext.opengraph",
]

autodoc_default_options = {
    "members": True,
    "inherited-members": True,
    "inherit-docstrings": True,
    "member-order": "bysource",
}

ahead = 0

if ".post0.dev" in memcachio.__version__:
    version, ahead = memcachio.__version__.split(".post0.dev")
else:
    version = memcachio.__version__

release = version

html_title = f"{project} <small><b style='color: var(--color-brand-primary)'>{{{release}}}</b></small>"
try:
    ahead = int(ahead)

    if ahead > 0:
        html_theme_options[
            "announcement"
        ] = f"""
        This is a development version. The documentation for the latest version: <b>{latest_version or release}</b> can be found <a href="/en/stable">here</a>
        """
        html_title = f"{project} <small><b style='color: var(--color-brand-primary)'>{{dev}}</b></small>"
except:
    pass

add_module_names = False
autodoc_typehints_format = "short"
autodoc_typehints = "both"
autodoc_preserve_defaults = True
autodoc_type_aliases = {
    "KeyT": "~memcachio.types.KeyT",
    "ValueT": "~memcachio.types.ValueT",
    "UnixSocketLocator": "~memcachio.types.UnixSocketLocator",
}
autosectionlabel_maxdepth = 3
autosectionlabel_prefix_document = True

extlinks = {
    "pypi": ("https://pypi.org/project/%s", "%s"),
}


issues_github_path = "alisaifee/memcachio"


htmlhelp_basename = "memcachiodoc"
latex_elements = {}

latex_documents = [
    (master_doc, "memcachio.tex", "memachio Documentation", "alisaifee", "manual"),
]
man_pages = [(master_doc, "memcachio", "memachio Documentation", [author], 1)]

texinfo_documents = [
    (
        master_doc,
        "memcachio",
        "memcachio Documentation",
        author,
        "memcachio",
        "One line description of project.",
        "Miscellaneous",
    ),
]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Workaround for https://github.com/sphinx-doc/sphinx/issues/9560
from sphinx.domains.python import PythonDomain
from sphinx.ext.autodoc import ClassDocumenter, Documenter, _

assert PythonDomain.object_types["data"].roles == ("data", "obj")
PythonDomain.object_types["data"].roles = ("data", "class", "obj")

# Workaround for https://github.com/sphinx-doc/sphinx/issues/10333
from sphinx.util import inspect

inspect.TypeAliasForwardRef.__repr__ = lambda self: self.name
inspect.TypeAliasForwardRef.__hash__ = lambda self: hash(self.name)

ClassDocumenter.get_overloaded_signatures = lambda *_: []

