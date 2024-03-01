from setuptools import find_packages, setup
import json
import os


def read_pipenv_dependencies(fname):
    """Get default dependencies from Pipfile.lock."""
    filepath = os.path.join(os.path.dirname(__file__), fname)
    with open(filepath) as lockfile:
        lockjson = json.load(lockfile)
        return [dependency for dependency in lockjson.get('default')]

setup(
    name="DeltaPySpark",
    version="3.1.0",
    packages=find_packages(),
    description='Delta Lake package',
    entry_points={
        'group_1': 'run=DeltaPySpark.__main__:main'
    },
)
