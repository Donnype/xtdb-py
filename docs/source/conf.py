# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import subprocess

branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode("utf-8")
commit_date = subprocess.check_output(["git", "log", "--format=#%h %cs", "-n 1"]).decode("utf-8")

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "XTDB-Python"
copyright = "Donny Peeters (MIT License)"
author = "Donny Peeters"
version = branch
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_rtd_theme",
    "myst_parser",
]

myst_enable_extensions = ["tasklist"]

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_logo = "_static/logo.png"
html_favicon = "_static/favicon.svg"
html_context = {
    "display_github": True,
    "github_user": "DonnyPe",
    "github_repo": "xtdb-py",
    "github_version": "main",
    "conf_py_path": "/docs/source/",
}

html_static_path = ["_static"]
