from setuptools import setup

setup(name="saddle",
    version="0.1",
    scripts=["bin/saddle"],
    description="Saddle, for tonight we ride!",
    url="http://github.com/btoll/saddle",
    author="btoll",
    author_email="benjam72@yahoo.com",
    license="GPLv3+",
    packages=["saddle"],
    include_package_data=True,
    install_requires=[
        "json",
        "mulecli",
        "pyyaml",
    ],
    classifiers=[
        # https://pypi.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)"
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    zip_safe=False)

