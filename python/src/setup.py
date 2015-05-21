import setuptools


# To debug, set DISTUTILS_DEBUG env var to anything.
setuptools.setup(
    name="mapreduce",
    version="1.1.0",
    packages=setuptools.find_packages(),
    author="Kevin Sookocheff",
    author_email="ksookocheff@vendasta.com",
    url="https://github.com/vendasta/appengine-mapreduce.git",
    keywords="google app engine mapreduce data processing",
    license="Apache License 2.0",
    description="Enable MapReduce style data processing on App Engine",
    zip_safe=True,
    # Include package data except README.
    include_package_data=True,
    exclude_package_data={"": ["README"]},
    install_requires=[
        "GoogleAppEngineCloudStorageClient >= 1.9.15",
        "pipeline >= 1.1.0",
        "Graphy >= 1.0.0",
        "simplejson >= 3.6.5",
        ]
)
