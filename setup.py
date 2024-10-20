# setup.py
from setuptools import setup, find_packages

setup(
    name='job-queue',
    version='0.1',
    packages=find_packages(),
    author='DumbAI',
    description='A job queue for processing tasks',

    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/DumbAI/job-queue',

    install_requires=[
        'boto3',
        'botocore',
        'pydantic',
    ],
)
