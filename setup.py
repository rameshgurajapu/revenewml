from setuptools import setup, find_packages
import os

setup(
    name='revenewML',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_dir=os.getcwd(),
    install_requires=[
        'Click',
        'pandas',
        'shap',
        'SQLAlchemy',
        'tqdm',
        'xgboost'
    ],
    entry_points='''
        [console_scripts]
        revenewML=src.scoring:main
    ''',
)
