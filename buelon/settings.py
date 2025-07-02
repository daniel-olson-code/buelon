import os
import yaml


try:
    import dotenv
    dotenv.load_dotenv(os.environ.get('ENV_PATH', '.env'))
except ModuleNotFoundError:
    pass


DIR_PATH = os.environ.get('BUELON_DIR_PATH', '.bue')
SETTINGS_PATH = os.environ.get('BUELON_SETTINGS_PATH', os.path.join(DIR_PATH, 'settings.yaml'))

# Environment variables for client and server configuration
USING_POSTGRES: bool = os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

REDIS_HOST: str = os.environ.get('REDIS_HOST', 'null')
REDIS_PORT: int = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB: int = int(os.environ.get('REDIS_DB', 0))
REDIS_EXPIRATION: int | None = os.environ.get('REDIS_EXPIRATION', 60*60*24*7)
try:
    REDIS_EXPIRATION = int(REDIS_EXPIRATION)
except ValueError:
    REDIS_EXPIRATION = None
USING_REDIS: bool = os.environ.get('USING_REDIS', 'false') == 'true'  # False  # REDIS_HOST != 'null'

BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

USING_ZOOKEEPER = os.environ.get('USING_ZOOKEEPER', 'false') == 'true'
ZOOKEEPER_HOSTS: str = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
ZOOKEEPER_PATH: str = f"{os.environ.get('ZOOKEEPER_PATH', '/buelon/bucket')}"

PERSISTENT_PATH: str = f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

DEFAULT_SETTINGS = {
    'hub': {
        'host': '0.0.0.0',
        'port': 65432,
        # 'username': 'XXXXX',
        # 'password': 'XXXXX'
    },
    'worker': {
        'host': 'localhost',
        'port': 65432,
        'scopes': 'production-very-heavy,production-heavy,production-medium,production-small,testing-heavy,testing-medium,testing-small,default',
        # 'subprocess': False,
        # 'n_processes': 1,
        # 'n_threads': 1,
        # 'n_jobs': 1,
        # 'job_timeout': 60 * 60 * 2,
        # 'restart_interval': 60 * 60 * 2,
        'reverse': False,
        # 'one_shot': False,
    },
    'bucket': {
        'server': {
            'use': True,
            'path': os.path.join(DIR_PATH, 'bucket'),
            'host': '0.0.0.0',
            'port': 61535
            # 'max_size': 1024 * 1024 * 1024 * 1024,  # 1 TB
        },
        'client': {
            'use': True,
            'host': 'localhost',
            'port': 61535,
            # 'timeout': 60,
            # 'max_size': 1024 * 1024 * 1024 * 1024,  # 1 TB
        },
        'postgres': {
            'use': False,
            'table': 'buelon_bucket',
            'persistent_path': '__PERSISTENT__',
        },
    },
    'postgres': {
        'host': 'localhost',
        'port': 5432,
        'username': 'XXXXX',
        'password': 'XXXXX',
        'database': 'XXXXX',
        # 'schema': 'XXXXX',
    },
    # 'redis': {
    #     'host': 'localhost',
    #     'port': 6379,
    #     'db': 0,
    #     'password': 'XXXXX',
    # },
    # 'logging': {
    #     'level': 'INFO',
    #     'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
    # },
}


class YamlObj:
    def convert(self, v):
        if isinstance(v, YamlObj):
            return v.get_dict()
        if isinstance(v, dict):
            return {key: self.convert(val) for key, val in v.items()}
        if isinstance(v, list):
            return [self.convert(val) for val in v]
        return v

    def get_dict(self):
        return self.convert(self.__dict__)

    def __str__(self):
        return yaml.dump(self.get_dict(), default_flow_style=False)


class HubSettings(YamlObj):
    def __init__(self, settings: dict):
        self.host = settings.get('host', DEFAULT_SETTINGS['hub']['host'])
        self.port = settings.get('port', DEFAULT_SETTINGS['hub']['port'])
        # self.username = settings.get('username', DEFAULT_SETTINGS['hub']['username'])
        # self.password = settings.get('password', DEFAULT_SETTINGS['hub']['password'])


class WorkerSettings(YamlObj):
    def __init__(self, settings: dict):
        self.host = settings.get('host', DEFAULT_SETTINGS['worker']['host'])
        self.port = settings.get('port', DEFAULT_SETTINGS['worker']['port'])
        self.scopes = settings.get('scopes', DEFAULT_SETTINGS['worker']['scopes'])
        # self.subprocess = settings.get('subprocess', DEFAULT_SETTINGS['worker']['subprocess'])
        # self.n_processes = settings.get('n_processes', DEFAULT_SETTINGS['worker']['n_processes'])
        # self.n_threads = settings.get('n_threads', DEFAULT_SETTINGS['worker']['n_threads'])
        # self.n_jobs = settings.get('n_jobs', DEFAULT_SETTINGS['worker']['n_jobs'])
        # self.job_timeout = settings.get('job_timeout', DEFAULT_SETTINGS['worker']['job_timeout'])
        # self.restart_interval = settings.get('restart_interval', DEFAULT_SETTINGS['worker']['restart_interval'])
        self.reverse = settings.get('reverse', DEFAULT_SETTINGS['worker']['reverse'])


class BucketServerSettings(YamlObj):
    def __init__(self, settings: dict):
        self.use = settings.get('use', DEFAULT_SETTINGS['bucket']['server']['use'])
        self.path = settings.get('path', DEFAULT_SETTINGS['bucket']['server']['path'])
        self.host = settings.get('host', DEFAULT_SETTINGS['bucket']['server']['host'])
        self.port = settings.get('port', DEFAULT_SETTINGS['bucket']['server']['port'])


class BucketClientSettings(YamlObj):
    def __init__(self, settings: dict):
        self.use = settings.get('use', DEFAULT_SETTINGS['bucket']['client']['use'])
        self.host = settings.get('host', DEFAULT_SETTINGS['bucket']['client']['host'])
        self.port = settings.get('port', DEFAULT_SETTINGS['bucket']['client']['port'])


class BucketPostgresSettings(YamlObj):
    def __init__(self, settings: dict):
        self.use = settings.get('use', DEFAULT_SETTINGS['bucket']['postgres']['use'])
        self.table = settings.get('table', DEFAULT_SETTINGS['bucket']['postgres']['table'])
        self.persistent_path = settings.get('persistent_path', DEFAULT_SETTINGS['bucket']['postgres']['persistent_path'])


class BucketSettings(YamlObj):
    def __init__(self, settings: dict):
        self.server = BucketServerSettings(settings.get('server', DEFAULT_SETTINGS['bucket']['server']))
        self.client = BucketClientSettings(settings.get('client', DEFAULT_SETTINGS['bucket']['client']))
        self.postgres = BucketPostgresSettings(settings.get('postgres', DEFAULT_SETTINGS['bucket']['postgres']))


class PostgresSettings(YamlObj):
    def __init__(self, settings: dict):
        self.host = settings.get('host', DEFAULT_SETTINGS['postgres']['host'])
        self.port = settings.get('port', DEFAULT_SETTINGS['postgres']['port'])
        self.username = settings.get('username', DEFAULT_SETTINGS['postgres']['username'])
        self.password = settings.get('password', DEFAULT_SETTINGS['postgres']['password'])
        self.database = settings.get('database', DEFAULT_SETTINGS['postgres']['database'])
        # self.schema = settings.get('schema', DEFAULT_SETTINGS['postgres']['schema'])


class BuelonSettings(YamlObj):
    def __init__(self, settings: dict):
        self.hub = HubSettings(settings.get('hub', DEFAULT_SETTINGS['hub']))
        self.worker = WorkerSettings(settings.get('worker', DEFAULT_SETTINGS['worker']))
        self.bucket = BucketSettings(settings.get('bucket', DEFAULT_SETTINGS['bucket']))
        self.postgres = PostgresSettings(settings.get('postgres', DEFAULT_SETTINGS['postgres']))
        # self.redis = RedisSettings(settings.get('redis', DEFAULT_SETTINGS['redis']))


if os.path.exists(SETTINGS_PATH):
    with open(SETTINGS_PATH, 'r') as f:
        settings = BuelonSettings(yaml.safe_load(f))
else:
    settings = BuelonSettings(DEFAULT_SETTINGS)


def init():
    if not os.path.exists(DIR_PATH):
        os.makedirs(DIR_PATH)

    if not os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, 'w') as f:
            f.write(str(settings))


