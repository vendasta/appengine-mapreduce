from setuptools import setup, find_packages

setup(
    name="mapreduce",
    version="1.0.0",
    packages=find_packages(),
    author="VendAsta",
    author_email="jcollins@vendasta.com",
    keywords="google app engine mapreduce data processing",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    license="Apache License 2.0",
    description="Enable MapReduce style data processing on App Engine",
    include_package_data=True,
    install_requires=[
            "GoogleAppEngineCloudStorageClient >= 1.9.5",
            "pipeline >= 1.0.0",
            "Graphy >= 1.0.0",
            "simplejson == 2.1.1",
            "mock >= 1.0.1",
            "mox >= 0.5.3",
        ]
)
