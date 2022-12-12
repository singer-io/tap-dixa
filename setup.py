from setuptools import find_packages, setup

requirements_file = "requirements.txt"

setup(
    name="tap-dixa",
    version="1.0.2",
    description="Singer.io tap for extracting data from the Dixa API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dixa"],
    install_requires=[
        "backoff==1.8.0",
        "certifi==2021.5.30",
        "charset-normalizer==2.0.4",
        "ciso8601==2.1.3",
        "idna==3.2",
        "jsonschema==2.6.0",
        "python-dateutil==2.8.2",
        "pytz==2018.4",
        "requests==2.26.0",
        "simplejson==3.11.1",
        "singer-python==5.12.1",
        "six==1.16.0",
        "urllib3==1.26.6",
    ],
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
