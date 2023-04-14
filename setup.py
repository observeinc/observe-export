from setuptools import setup, find_packages

install_requires = [
    "click",
    "numpy",
    "pandas",
    "psutil",
    "dataclasses-json",
    "openpyxl",
	"sphinx",
	"sphinx-markdown-builder",
	"sphinx-click",
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
