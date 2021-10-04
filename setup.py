from setuptools import find_packages, setup

requirements_file = "requirements.txt"

setup(
    name="tap-dixa",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Dixa API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dixa"],
    install_requires=open(requirements_file).readlines(),
    entry_points="""
    [console_scripts]
    tap-dixa=tap_dixa:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data = {
        "schemas": ["tap_dixa/schemas/*.json"]
    },
    include_package_data=True,
)
