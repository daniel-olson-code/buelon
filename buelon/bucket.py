"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.

STANDALONE: the bucket is no longer part of the hub/worker path. The hub keeps all of
its state (jobs, results, holds) in memory and workers talk only to the hub, so nothing
in ``buelon.hub``, ``buelon.worker`` or ``buelon.core`` imports this module. It is kept
as a standalone key/value store, reachable on its own via ``bue bucket``.
"""
from buelon.settings import settings
from buelon.bucket_v1 import *


# Environment variables for client and server configuration
USING_POSTGRES: bool = settings.bucket.postgres.use  # os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = settings.bucket.postgres.table  # os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

BUCKET_CLIENT_HOST: str = settings.bucket.client.host  # os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = settings.bucket.client.port  # int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = settings.bucket.server.host  # os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = settings.bucket.server.port  # int(os.environ.get('BUCKET_SERVER_PORT', 61535))

PERSISTENT_PATH: str = settings.bucket.postgres.persistent_path  # f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = settings.bucket.server.path  # os.path.join('.bue', 'bucket')

database: dict[str, bytes] = {}
database_keys_in_order = []
# MAX_DATABASE_SIZE: int = min(1024 * 1024 * 1024 * 1, int(psutil.virtual_memory().total / 8))
MAX_DATABASE_SIZE: int = 50 * 1024 * 1024

if not USING_POSTGRES:
    if not os.path.exists(save_path):
        os.makedirs(save_path)


if __name__ == '__main__':
    main()

