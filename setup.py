#!/usr/bin/env python3

from setuptools import setup

setup(name='Splark',
      version='0.1.0',
      description='Extensible Cluster Computing Framework',
      url='https://github.com/belisarius222/splark/',
      packages=['splark', 'splark.worker', 'splark.tests'],
      keywords=['spark','splark','clustering','bigdata','big data', \
                'distributed processing'],
      classifiers = [
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Clustering",
        "Topic :: System :: Distributed Computing",
        ],
        long_description = """
        Splark is a fast and general cluster computing system for Big Data. It 
        is being desgined and built with extensibility in mind.
        """
     )
