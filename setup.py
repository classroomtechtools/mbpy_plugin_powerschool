from setuptools import setup

setup(
    name='powerschool-asw',
    version='0.1',
    modules=['powerschool_asw'],
    install_requires=[
        'click',
    ],
    entry_points='''
        [mbpy_plugins]
        sync-asw=powerschool_asw.cli:sync
    ''',
)
