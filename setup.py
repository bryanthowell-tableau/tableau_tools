from setuptools import setup, find_packages

setup(
    name='tableau_tools',
    version='4.6.0',
    packages=find_packages(),
    url='https://github.com/bryantbhowell/tableau_tools',
    license='',
    author='Bryant Howell',
    author_email='bhowell@tableau.com',
    description='A library for programmatically working with Tableau files and Tableau Server, including the REST API',
    install_requires=['requests'],
    use_2to3=True
)
