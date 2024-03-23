from setuptools import setup, find_packages
#from package import Package

with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup(name='AIQSales',
      version='0.0.1',
      description='A AIQ Sales data ingestion pipelines',
      author='Dharmendra Kumar',
      author_email='drathore586@gmail.com',
      install_requires= ['requests==2.24.0', 'pandas==1.1.1', 'psycopg2==2.8.6', 
      'sqlalchemy==1.2.7', 'aiohttp==3.7.3', 'asyncio==3.4.3' ],
      packages=find_packages(),
      zip_safe=False)  