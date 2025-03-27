from __future__ import annotations

import os
import pathlib
import platform
import sys

import versioneer

__author__ = "Ali-Akber Saifee"
__email__ = "ali@indydevs.org"
__copyright__ = "Copyright 2025, Ali-Akber Saifee"

from setuptools import find_packages, setup

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
PY_IMPLEMENTATION = platform.python_implementation()
USE_MYPYC = False
PURE_PYTHON = os.environ.get("PURE_PYTHON", PY_IMPLEMENTATION != "CPython")


def get_requirements(req_file):
    requirements = []

    for r in open(os.path.join(THIS_DIR, "requirements", req_file)).read().splitlines():
        req = r.strip()

        if req.startswith("-r"):
            requirements.extend(get_requirements(req.replace("-r ", "")))
        elif req:
            requirements.append(req)

    return requirements


_ROOT_DIR = pathlib.Path(__file__).parent

with open(str(_ROOT_DIR / "README.md")) as f:
    long_description = f.read()

if len(sys.argv) > 1 and "--use-mypyc" in sys.argv:
    sys.argv.remove("--use-mypyc")
    USE_MYPYC = True

extensions = []
if not PURE_PYTHON and USE_MYPYC:
    from mypyc.build import mypycify

    extensions += mypycify(
        [],
        debug_level="0",
        strip_asserts=True,
    )
    for ext in extensions:
        if "-Werror" in ext.extra_compile_args:
            ext.extra_compile_args.remove("-Werror")

setup(
    name="memcachio",
    version=versioneer.get_version(),
    description="Python async client for Memcached",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alisaifee/memcachio",
    project_urls={
        "Source": "https://github.com/alisaifee/memcachio",
        "Changes": "https://github.com/alisaifee/memcachio/releases",
        "Documentation": "https://memcachio.readthedocs.org",
    },
    author=__author__,
    author_email=__email__,
    maintainer=__author__,
    maintainer_email=__email__,
    keywords=["Memcached", "asyncio"],
    license="MIT",
    packages=find_packages(exclude=["*tests*"]),
    include_package_data=True,
    package_data={
        "memcachio": ["py.typed"],
    },
    python_requires=">=3.11",
    install_requires=get_requirements("main.txt"),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    ext_modules=extensions,
)
