from setuptools import setup

setup(
    name='minio-watcher',
    version='1.0.0',
    install_requires=[
        'elasticsearch',
        'minio',
        'pika',
        'python-dotenv'
    ],
    py_modules=['minio_watcher']
)
