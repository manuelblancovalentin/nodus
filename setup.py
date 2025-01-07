from setuptools import setup, find_packages

setup(
    name="nodus",
    version="0.0.8",
    author="Manuel Blanco Valentin",
    author_email="manuel.blanco.valentin@gmail.com",
    description="Nodus - A lightweight and reusable job manager.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/manuelblancovalentin/nodus",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)