#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2009 Atommica. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The setup and build script for the pyrant library.
"""

import os
from setuptools import setup, find_packages

import pyrant

readme = open(os.path.join(os.path.dirname(__file__), 'README')).read()

setup(
    name = "pyrant",
    version = pyrant.__version__,
    url = 'http://code.google.com/p/pyrant/',
    license = 'Apache License 2.0',
    description = 'A python wrapper around Tokyo Tyrant',
    long_description = readme,
    author = 'Martin Conte Mac Donell',
    author_email = 'Reflejo@gmail.com',
    packages = find_packages(),
    install_requires = ['setuptools'],
    include_package_data = True,
    classifiers = [
      'Intended Audience :: Developers',
      'Development Status :: 4 - Beta',
      'Programming Language :: Python',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: Apache Software License',
      'Topic :: Software Development :: Libraries :: Python Modules',
      'Topic :: Database :: Front-Ends',
    ],
)
