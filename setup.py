from setuptools import setup

setup(
    name='mbpy_plugin_powerschool',
    version='0.1',
    modules=['powerschool'],
    install_requires=[
        'click',
    ],
    entry_points='''
        [mbpy_plugins]
        sync-asw=powerschool.cli:sync
    ''',
)
