from os.path import dirname, join
from setuptools import setup, find_packages

version = "2.6.3"

dir = dirname(__file__)

with open(join(dir, "requirements.txt"), "r") as f:
    install_requires = [line.rstrip("\n") for line in f.readlines()]

setup(
    include_package_data=True,
    name="metaflow",
    version=version,
    description="Metaflow: More Data Science, Less Engineering",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Machine Learning Infrastructure Team at Netflix",
    author_email="help@metaflow.org",
    license="Apache License 2.0",
    packages=find_packages(exclude=["metaflow_test"]),
    py_modules=[
        "metaflow",
    ],
    package_data={"metaflow": ["tutorials/*/*"]},
    entry_points="""
        [console_scripts]
        metaflow=metaflow.main_cli:main
      """,
    install_requires=install_requires,
    tests_require=["coverage"],
)
