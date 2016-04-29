#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import inspect

from setuptools import setup, find_packages

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))

NAME = 'pgobserver_gatherer'
MAIN_MODULE = 'pgobserver_gatherer'
VERSION = '0.1.1'
DESCRIPTION = 'PostgreSQL metrics gathering daemon'
LICENSE = 'Apache License 2.0'
URL = 'https://github.com/zalando/PGObserver'
AUTHOR = 'kmoppel'
EMAIL = 'kaarel.moppel@zalando.de'
KEYWORDS = 'postgres postgresql pg database monitoring'

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: Implementation :: CPython',
    'Topic :: Database'
]

CONSOLE_SCRIPTS = ['pgobserver_gatherer = pgobserver_gatherer.run:main']


def get_install_requirements(path):
    content = open(os.path.join(__location__, path)).read()
    return [req for req in content.split('\\n') if req != '']


def read(fname):
    return open(os.path.join(__location__, fname)).read()


def setup_package():

    install_reqs = get_install_requirements('requirements.txt')

    setup(
        name=NAME,
        version=VERSION,
        url=URL,
        description=DESCRIPTION,
        author=AUTHOR,
        author_email=EMAIL,
        license=LICENSE,
        keywords=KEYWORDS,
        long_description=read('README.rst'),
        classifiers=CLASSIFIERS,
        py_modules=['pgobserver_gatherer'],
        packages=find_packages(exclude=['tests', 'tests.*']),
        install_requires=install_reqs,
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    setup_package()
