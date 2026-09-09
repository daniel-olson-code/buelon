"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.

STANDALONE: the bucket is no longer part of the hub/worker path. The hub keeps all of
its state (jobs, results, holds) in memory and workers talk only to the hub, so nothing
in ``buelon.hub``, ``buelon.worker`` or ``buelon.core`` imports this module. It is kept
as a standalone key/value store, reachable on its own via ``bue bucket``.
"""
from buelon.bucket_v1 import *
import buelon.bucket_v1


# `bucket_v1` reads `buelon.settings` directly, so everything this module used to
# rebind from `settings` -- `USING_POSTGRES`, `POSTGRES_TABLE`, `BUCKET_CLIENT_HOST/PORT`,
# `BUCKET_SERVER_HOST/PORT`, `PERSISTENT_PATH`, `save_path` -- now arrives through the
# star import above with the settings already applied. The rebindings are gone: they
# shadowed `bucket_v1`'s copies rather than replacing them, and it is `bucket_v1`'s that
# `Client`, `Server` and the `server_*` functions actually resolve. BUGS.md #63.


def ensure_save_path() -> None:
    """Create the bucket's storage directory, if the file backend is in use.

    This used to run at module scope, which meant a bare ``import buelon`` created
    ``.boo/bucket`` in whatever directory it happened to run from -- the last of the
    import-time ``os.makedirs`` calls #55 removed elsewhere. `save_path` has no other
    reader in this module, so the creation belongs next to the server that writes
    there. BUGS.md #62.

    Both values are read off ``buelon.bucket_v1`` rather than off this module's
    star-imported copies, so the directory made here is the one ``handle_client``
    writes to even if something rebinds them at runtime. BUGS.md #63.
    """
    if buelon.bucket_v1.USING_POSTGRES:
        return
    os.makedirs(buelon.bucket_v1.save_path, exist_ok=True)


def main() -> None:
    """Run the bucket server (`bue bucket`), creating its storage directory first."""
    ensure_save_path()
    buelon.bucket_v1.main()


if __name__ == '__main__':
    main()

