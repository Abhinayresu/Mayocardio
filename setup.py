from setuptools import setup, find_packages

setup(
    name="mayacardio-pipeline",
    version="1.0.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "pipeline=pipeline.cli:main",
            "biosignal-mock=biosignal_mock.mock_service:main",
            "check_ground_truth=biosignal_mock.check_ground_truth:main",
        ],
    },
    python_requires=">=3.9",
)
