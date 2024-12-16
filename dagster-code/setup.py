from setuptools import find_packages, setup

setup(
    name="dagster_code",
    packages=find_packages(exclude=["dagster_code_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "clickhouse_connect",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
