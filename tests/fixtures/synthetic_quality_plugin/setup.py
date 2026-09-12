from setuptools import find_packages, setup

setup(
    name="sdp-meta-synthetic-quality-plugin",
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        "databricks.labs.sdp_meta.quality_engines": [
            "synthetic=synthetic_quality_plugin:SyntheticQualityEngine"
        ]
    },
)
