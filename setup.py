from setuptools import setup, find_packages

version = '2.2.8'
tests_require = [
    'coverage',
    'pandas',
    'pytest',
    'tox',
]

setup(name='metaflow',
      version=version,
      description='Metaflow: More Data Science, Less Engineering',
      author='Machine Learning Infrastructure Team at Netflix',
      author_email='help@metaflow.org',
      license='Apache License 2.0',
      packages=find_packages(exclude=['metaflow_test']),
      py_modules=['metaflow', ],
      package_data={'metaflow' : ['tutorials/*/*']},
      entry_points='''
        [console_scripts]
        metaflow=metaflow.main_cli:main
      ''',
      install_requires = [
        'click>=7.0,<8',  # "TypeError: expected str, bytes or os.PathLike object, not function" in Click 8.0, metaflow/includefile.py:230: in convert
        'requests',
        'boto3',
        'pylint<2.5.0',
      ],
      extras_require = { 'test': tests_require },
      tests_require = tests_require)
