"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.
"""
from buelon.settings import settings
from buelon.bucket_v1 import *


# Environment variables for client and server configuration
USING_POSTGRES: bool = settings.bucket.postgres.use  # os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = settings.bucket.postgres.table  # os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

REDIS_HOST: str = os.environ.get('REDIS_HOST', 'null')
REDIS_PORT: int = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB: int = int(os.environ.get('REDIS_DB', 0))
REDIS_EXPIRATION: int | None = os.environ.get('REDIS_EXPIRATION', 60*60*24*7)
try:
    REDIS_EXPIRATION = int(REDIS_EXPIRATION)
except ValueError:
    REDIS_EXPIRATION = None
USING_REDIS: bool = os.environ.get('USING_REDIS', 'false') == 'true'  # False  # REDIS_HOST != 'null'

BUCKET_CLIENT_HOST: str = settings.bucket.client.host  # os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = settings.bucket.client.port  # int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = settings.bucket.server.host  # os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = settings.bucket.server.port  # int(os.environ.get('BUCKET_SERVER_PORT', 61535))

USING_ZOOKEEPER = os.environ.get('USING_ZOOKEEPER', 'false') == 'true'
ZOOKEEPER_HOSTS: str = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
ZOOKEEPER_PATH: str = f"{os.environ.get('ZOOKEEPER_PATH', '/buelon/bucket')}"

PERSISTENT_PATH: str = settings.bucket.postgres.persistent_path  # f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = settings.bucket.server.path  # os.path.join('.bue', 'bucket')

database: dict[str, bytes] = {}
database_keys_in_order = []
# MAX_DATABASE_SIZE: int = min(1024 * 1024 * 1024 * 1, int(psutil.virtual_memory().total / 8))
MAX_DATABASE_SIZE: int = 50 * 1024 * 1024

if not USING_REDIS and not USING_POSTGRES and not USING_ZOOKEEPER:
    if not os.path.exists(save_path):
        os.makedirs(save_path)


if __name__ == '__main__':
    main()

