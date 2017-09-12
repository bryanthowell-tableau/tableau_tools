from setuptools import setup

setup(
    name='tableau_tools',
    version='4.0.1',
    packages=['tableau_tools', 'tableau_tools.tableau_rest_api', 'tableau_tools.tableau_documents'],
    url='https://github.com/bryantbhowell/tableau_tools',
    license='',
    author='Bryant Howell',
    author_email='bhowell@tableau.com',
    description='A library for programmatically working with Tableau files and Tableau Server, including the REST API'
)
