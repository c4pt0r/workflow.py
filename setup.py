from setuptools import setup, find_packages

setup(
    name="workflowlite",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
    ],
    author="Ed Huang",
    author_email="dongxuhuang@yahoo.com",
    description="a simple workflow engine",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/c4pt0r/workflowlite",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

