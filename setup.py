from setuptools import setup, find_packages

install_requires = [
    "click",
    "matplotlib",
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
    name="observe_csv",
    version="0.0.1",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "observe-csv = observe_csv.observe_csv:cli",
        ]
    }
)
