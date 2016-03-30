from setuptools import setup

setup(
  name='PGH',
  description='PGH is a command line tool to help you monitor and debug your PostgreSQL database.',
  long_description='PGH is a command line tool to help you monitor and debug your PostgreSQL database.',
  author='André Barbosa',
  author_email = "albmail88@gmail.com",
  license = "MIT",
  keywords = "pg postgres postgresql",
  url = "https://github.com/nata79/pgh",
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
