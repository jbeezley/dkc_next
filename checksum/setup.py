from setuptools import find_packages, setup

setup(
    name='checksum-service',
    version='1.0.0',
    install_requires=[
        'click',
        'elasticsearch',
        'flask',
        'gunicorn',
        'minio',
        'pika',
        'python-dotenv'
    ],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'checksum-service = checksum_service.cli:main'
        ]
    }
)
