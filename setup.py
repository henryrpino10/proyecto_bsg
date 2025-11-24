"""
Setup para el paquete YOLO Detection Pipeline.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Leer README
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Leer requirements
requirements = (this_directory / "requirements.txt").read_text(encoding="utf-8").splitlines()
requirements = [r.strip() for r in requirements if r.strip() and not r.startswith("#")]

dev_requirements = (this_directory / "requirements-dev.txt").read_text(encoding="utf-8").splitlines()
dev_requirements = [r.strip() for r in dev_requirements if r.strip() and not r.startswith("#")]

setup(
    name="yolo-detection-pipeline",
    version="1.0.0",
    author="henry Rios",
    author_email="henry.rpino@gmail.com",
    description="Pipeline end-to-end para detección de objetos con YOLO y ETL a Hive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/henryrpino10/proyecto_bsg",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    entry_points={
        "console_scripts": [
            "yolo-classify=scripts.run_classification:main",
            "yolo-etl=scripts.run_etl:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml"],
    },

)

