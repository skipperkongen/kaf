import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='kaf',
     version='v0.2.1',
     download_url='https://github.com/skipperkongen/kaf/archive/v0.2.1.tar.gz',
     licence='MIT',
     author="Pimin Konstantin Kefaloukos",
     author_email="skipperkongen@gmail.com",
     description="A mini-framework for Kafka apps",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/skipperkongen/kaf",
     packages=['kaf'],
     install_requires=[
        'confluent_kafka',
        'retrying'
     ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )
