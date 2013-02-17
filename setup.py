from setuptools import setup, find_packages


version = '0.1.0-dev'


setup(name='memcrashed',
      version=version,
      description="A Memcached sharding and failover proxy",
      long_description="""\
A Memcached sharding and failover proxy""",
      classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='memcashed proxy sharding failover',
      author='Diogo Baeder',
      author_email='contato@diogobaeder.com.br',
      url='https://github.com/diogobaeder/memcrashed',
      license='BSD 2-Clause',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'tornado',
      ],
      entry_points={
          'console_scripts': [
              'memcrashed = memcrashed.server:main',
          ],
      },
      )
