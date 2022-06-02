from setuptools import setup, find_packages

setup(
    name="trade-db-etl",
    version='1.0.0',
    author="shreeshma",
    author_email="sbaimeedi@galaxe.com",
    description="Ingest csv data from s3 to postgres ",
    long_description_content_type="text/markdown",
    url="",
    license='',
    packages=find_packages(),
    scripts=[],
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        'boto3',
        'fake-awsglue',
        'pyspark',
        'psycopg2'
    ],
    python_requires='>=3.6',
    zip_safe=False,
)
