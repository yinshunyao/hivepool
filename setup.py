#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/5/12 12:06
# @Author  : ysy
# @Site    : 
# @File    : setup.py
# @Software: PyCharm
from distutils.core import setup
from setuptools import setup, find_packages

# 从readme中获取信息
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = 'hivepool',
    version = '1.0',
    keywords = ('hive', 'connection', 'pool'),
    description = 'hive pool based on pyhive',
    long_description = long_description,
    long_description_content_type="text/markdown",
    license = 'MIT License',
    url = 'https://github.com/yinshunyao/hivepool',
    author = 'yinshunyao',
    author_email = 'yinshunyao@qq.com',
    packages = find_packages(),
    install_requires=["PyHive", ],
    platforms = 'any',
    classifiers = [
        # "Development Status :: 5 - Production/Stable"
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)