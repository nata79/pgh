from setuptools import setup

setup(
  name='pgh',
  version='0.1',
  py_modules=['pgh'],
  install_requires=[
    'click',
    'tabulate',
    'psycopg2'
  ],
  entry_points='''
    [console_scripts]
    pgh=pgh:cli
  ''',
)
