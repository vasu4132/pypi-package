from setuptools import setup, find_packages
import versioneer

setup(
    name='pypipackage',
    packages=find_packages(exclude=["*.tests", "*.tests.*"]),
    include_package_data=True,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='The shared resources project',
    url='https://github.com/vasu4132/pypi-package.git',
    author='vasu',
    author_email='vasuchowdary413@gmail.com',
)
