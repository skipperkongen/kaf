import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='kaf',
     version='0.1',
     author="Pimin Konstantin Kefaloukos",
     author_email="skipperongen@gmail.com",
     description="A mini-framework for Kafka apps",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/skipperkongen/kaf",
     packages=setuptools.find_packages(),
     install_requires=[
        'confluent_kafka'
     ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )
