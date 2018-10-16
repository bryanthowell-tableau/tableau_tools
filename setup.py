from setuptools import setup

setup(
    name='tableau_tools',
    version='4.6.3',
    packages=['tableau_tools', 'tableau_tools.tableau_rest_api', 'tableau_tools.tableau_documents', 'tableau_tools.examples'],
    url='https://github.com/bryantbhowell/tableau_tools',
    license='',
    author='Bryant Howell',
    author_email='bhowell@tableau.com',
    description='A library for programmatically working with Tableau files and Tableau Server, including the REST API',
    install_requires=['requests'],
    use_2to3=True
)
