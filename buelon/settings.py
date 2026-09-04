import os
import yaml


try:
    import dotenv
    dotenv.load_dotenv(os.environ.get('ENV_PATH', '.env'))
except ModuleNotFoundError:
    pass


def _default_dir_path() -> str:
    """`.boo` is the state directory (BUGS.md #55). `.bue` is the pre-rename name.

    `buelon.migration` normally copies one to the other before this module is imported,
    so the fallback below only matters to someone who reached `buelon.settings` without
    going through `import buelon` -- or who turned the migration off with
    `BUELON_AUTO_MIGRATE=false`. Reading their existing `.bue/` is better than silently
    starting from an empty `.boo/`.
    """
    if not os.path.isdir('.boo') and os.path.isdir('.bue'):
        return '.bue'
    return '.boo'


_env_dir_path = os.environ.get('BUELON_DIR_PATH')
# Every path under the state directory goes through `DIR_PATH`; before #55 five modules
# hardcoded the literal `.bue`, so `BUELON_DIR_PATH` moved `settings.yaml` alone.
DIR_PATH = _env_dir_path if _env_dir_path is not None else _default_dir_path()
SETTINGS_PATH = os.environ.get('BUELON_SETTINGS_PATH', os.path.join(DIR_PATH, 'settings.yaml'))

# Environment variables for client and server configuration
USING_POSTGRES: bool = os.environ.get('USING_POSTGRES_BUCKET', 'false') == 'true'
POSTGRES_TABLE: str = os.environ.get('POSTGRES_TABLE', 'buelon_bucket')

BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

PERSISTENT_PATH: str = f"{os.environ.get('PERSISTENT_PATH', '__PERSISTENT__')}"

# bisocket's own default is 'secure', which runs bz2 *after* AES-GCM -- i.e. it
# compresses ciphertext, which is incompressible. buelon already bz2-compresses job
# batches at the application layer (`steps_to_compressed_message`), so that second pass
# is pure CPU on the hub thread. 'faster' skips it. BUGS.md #31.
DEFAULT_ENCRYPTION = 'faster'

DEFAULT_SETTINGS = {
    'hub': {
        'host': '0.0.0.0',
        'port': 65432,
        # Wire encryption for every hub/worker connection: 'secure' (AES-GCM then bz2),
        # 'faster' (AES-GCM only) or 'off' (plaintext). The hub and every worker MUST
        # agree -- a mismatch is refused, not negotiated. Leave it empty to let
        # $BISOCKET_ENCRYPTION decide.
        'encryption': DEFAULT_ENCRYPTION,
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
        'info': {
            'name': 'Worker',
        }
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
    # 'logging': {
    #     'level': 'INFO',
    #     'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
    # },
}


def _section(settings) -> dict:
    """A section written as `hub:` with an empty body parses as `None`, and an empty
    settings.yaml parses as `None` for the whole document. Treat either -- and anything
    else that is not a mapping -- as "absent", so the defaults apply instead of an
    `AttributeError` on `NoneType.get`. BUGS.md #38.
    """
    return settings if isinstance(settings, dict) else {}


def _get(settings: dict, key, default):
    """`settings.get(key, default)`, except that a present-but-null key falls back like an
    absent one, so `port:` with nothing after it means the default rather than `None`.
    BUGS.md #38.
    """
    value = settings.get(key, default)
    return default if value is None else value


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
        settings = _section(settings)
        self.host = _get(settings, 'host', DEFAULT_SETTINGS['hub']['host'])
        self.port = _get(settings, 'port', DEFAULT_SETTINGS['hub']['port'])
        # Read by both ends of the transport -- `bi_test_server`'s `BiServer` and
        # `BiWorkerClient`'s `BiClient` -- so there is one key to change rather than two
        # that can disagree. An empty value means "not set here": bisocket then falls
        # back to $BISOCKET_ENCRYPTION, and to its own 'secure' default. BUGS.md #31.
        # Deliberately `.get`, not `_get`: here a present-but-null `encryption:` is
        # meaningful ("defer to bisocket"), so it must NOT fall back to the default.
        _encryption = settings.get('encryption', DEFAULT_SETTINGS['hub']['encryption'])
        self.encryption = None if _encryption is None or _encryption == '' else _encryption
        # self.username = _get(settings, 'username', DEFAULT_SETTINGS['hub']['username'])
        # self.password = _get(settings, 'password', DEFAULT_SETTINGS['hub']['password'])


class WorkerSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.host = _get(settings, 'host', DEFAULT_SETTINGS['worker']['host'])
        self.port = _get(settings, 'port', DEFAULT_SETTINGS['worker']['port'])
        self.scopes = _get(settings, 'scopes', DEFAULT_SETTINGS['worker']['scopes'])
        # self.subprocess = _get(settings, 'subprocess', DEFAULT_SETTINGS['worker']['subprocess'])
        # self.n_processes = _get(settings, 'n_processes', DEFAULT_SETTINGS['worker']['n_processes'])
        # self.n_threads = _get(settings, 'n_threads', DEFAULT_SETTINGS['worker']['n_threads'])
        # self.n_jobs = _get(settings, 'n_jobs', DEFAULT_SETTINGS['worker']['n_jobs'])
        # self.job_timeout = _get(settings, 'job_timeout', DEFAULT_SETTINGS['worker']['job_timeout'])
        # self.restart_interval = _get(settings, 'restart_interval', DEFAULT_SETTINGS['worker']['restart_interval'])
        self.reverse = _get(settings, 'reverse', DEFAULT_SETTINGS['worker']['reverse'])

        _info = _get(settings, 'info', DEFAULT_SETTINGS['worker']['info'])
        # Copy, so a consumer renaming the worker (web.py) cannot mutate either
        # the parsed yaml or DEFAULT_SETTINGS itself.
        self.info = {} if not isinstance(_info, dict) else dict(_info)
        # `info` accepts any dict, so `info: {}` in settings.yaml would otherwise
        # leave consumers with a missing 'name'.
        self.info.setdefault('name', DEFAULT_SETTINGS['worker']['info']['name'])


class BucketServerSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.use = _get(settings, 'use', DEFAULT_SETTINGS['bucket']['server']['use'])
        self.path = _get(settings, 'path', DEFAULT_SETTINGS['bucket']['server']['path'])
        self.host = _get(settings, 'host', DEFAULT_SETTINGS['bucket']['server']['host'])
        self.port = _get(settings, 'port', DEFAULT_SETTINGS['bucket']['server']['port'])


class BucketClientSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.use = _get(settings, 'use', DEFAULT_SETTINGS['bucket']['client']['use'])
        self.host = _get(settings, 'host', DEFAULT_SETTINGS['bucket']['client']['host'])
        self.port = _get(settings, 'port', DEFAULT_SETTINGS['bucket']['client']['port'])


class BucketPostgresSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.use = _get(settings, 'use', DEFAULT_SETTINGS['bucket']['postgres']['use'])
        self.table = _get(settings, 'table', DEFAULT_SETTINGS['bucket']['postgres']['table'])
        self.persistent_path = _get(settings, 'persistent_path', DEFAULT_SETTINGS['bucket']['postgres']['persistent_path'])


class BucketSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.server = BucketServerSettings(_get(settings, 'server', DEFAULT_SETTINGS['bucket']['server']))
        self.client = BucketClientSettings(_get(settings, 'client', DEFAULT_SETTINGS['bucket']['client']))
        self.postgres = BucketPostgresSettings(_get(settings, 'postgres', DEFAULT_SETTINGS['bucket']['postgres']))


class PostgresSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.host = _get(settings, 'host', DEFAULT_SETTINGS['postgres']['host'])
        self.port = _get(settings, 'port', DEFAULT_SETTINGS['postgres']['port'])
        self.username = _get(settings, 'username', DEFAULT_SETTINGS['postgres']['username'])
        self.password = _get(settings, 'password', DEFAULT_SETTINGS['postgres']['password'])
        self.database = _get(settings, 'database', DEFAULT_SETTINGS['postgres']['database'])
        # self.schema = _get(settings, 'schema', DEFAULT_SETTINGS['postgres']['schema'])


class BuelonSettings(YamlObj):
    def __init__(self, settings: dict):
        settings = _section(settings)
        self.hub = HubSettings(_get(settings, 'hub', DEFAULT_SETTINGS['hub']))
        self.worker = WorkerSettings(_get(settings, 'worker', DEFAULT_SETTINGS['worker']))
        self.bucket = BucketSettings(_get(settings, 'bucket', DEFAULT_SETTINGS['bucket']))
        self.postgres = PostgresSettings(_get(settings, 'postgres', DEFAULT_SETTINGS['postgres']))


if os.path.exists(SETTINGS_PATH):
    with open(SETTINGS_PATH, 'r') as f:
        settings = BuelonSettings(yaml.safe_load(f) or {})
else:
    settings = BuelonSettings(DEFAULT_SETTINGS)


def init():
    if not os.path.exists(DIR_PATH):
        os.makedirs(DIR_PATH)

    if not os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, 'w') as f:
            f.write(str(settings))


