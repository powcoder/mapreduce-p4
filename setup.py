https://powcoder.com
代写代考加微信 powcoder
Assignment Project Exam Help
Add WeChat powcoder
"""
Mapreduce python modules.

Amy Chern <chernamy@umich.edu>
Nilay Muchhala <nilaym@umich.edu>
"""

from setuptools import setup

setup(
    name='mapreduce',
    version='0.1.0',
    packages=['mapreduce'],
    include_package_data=True,
    install_requires=[
        'click',
        'pycodestyle',
        'pydocstyle',
        'pylint',
        'pytest',
        'pytest-mock',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'mapreduce-manager = mapreduce.manager.__main__:main',
            'mapreduce-worker = mapreduce.worker.__main__:main',
            'mapreduce-submit = mapreduce.submit:main',
        ]
    },
)
