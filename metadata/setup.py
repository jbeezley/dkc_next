from setuptools import find_packages, setup

setup(
    name='metadata-service',
    version='1.0.0',
    install_requires=[
        'click',
        'elasticsearch',
        'flask',
        'gunicorn',
        'minio',
        'pika',
        'python-dateutil',
        'python-dotenv'
    ],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'metadata-service = metadata_service.cli:main'
        ]
    }
)
