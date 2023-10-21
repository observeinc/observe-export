from setuptools import setup, find_packages

install_requires = [
    "click~=8.1.0",
    "numpy~=1.24.0",
    "pandas<2.0.0",
    "psutil~=5.9.0",
    "dataclasses-json~=0.5.0",
    "openpyxl",
    "sphinx~=6.1.0",
    "sphinx-markdown-builder~=0.5.0",
    "sphinx-click~=4.4.0",
]

setup(
    name="observe_export",
    version="0.0.1",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "observe-export = observe_export.observe_export:cli",
        ]
    }
)
