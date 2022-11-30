from setuptools import setup, find_packages                                                                                                                                                          

setup(
    name='omnivore',
    version='0.1',
    packages=find_packages(exclude=['*tests*']),
    install_requires=[
        'usaddress',
        'pandas',
        'numpy',
        'simple-salesforce',
        'python-dotenv',
    ],
    zip_safe=False,
    
)