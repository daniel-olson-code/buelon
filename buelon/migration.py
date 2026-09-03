"""One-time state-directory migration: `.bue/` -> `.boo/`.  BUGS.md #55.

The project is named after Buelon Rexford Moss, whose nickname was **boo**, not `bue`.
`.bue/` was a misspelling, so the state directory is now `.boo/`. This module copies an
existing `.bue/` across on first run so a checkout that predates the rename keeps
working.

**Copy, not move.** `.bue/` is left exactly where it is, so downgrading to an older
buelon still finds its state. The cost is that the two directories drift apart from the
moment the copy happens -- that is the intended trade, and the printed message says so.

**Why this module imports nothing from buelon, and runs first.** `import buelon` used to
create `.bue/` as a side effect (`helpers/created_cache.py` ran `os.makedirs` at module
scope), which meant any "does the new directory already exist?" test run *after* the
package was imported answered yes, about a directory the import had just made. Two things
now keep that from happening:

  1. the helpers create their directories on first *use*, not at import; and
  2. `buelon/__init__.py` calls `migrate_bue_to_boo()` on its first line, before
     `buelon.settings` -- which resolves `DIR_PATH` at import time -- is imported at all.

(2) only holds while this module stays free of buelon imports, so keep it stdlib-only.

Deliberately **not** moved: `.auto_save`, the hub snapshot, which lives in a sibling
directory (`$BUELON_AUTO_SAVE_PATH`, default `.auto_save`) rather than inside `.bue/`.
"""
import os
import shutil

OLD_DIR_PATH = '.bue'
NEW_DIR_PATH = '.boo'


def _auto_migrate_enabled() -> bool:
    """`BUELON_AUTO_MIGRATE=false` opts out, matching `BUELON_AUTO_SAVE` (#48)."""
    return os.environ.get('BUELON_AUTO_MIGRATE', 'true').strip().lower() != 'false'


def _has_contents(path: str) -> bool:
    """True only for a directory with at least one entry in it.

    Existence is the wrong question: an empty `.bue/` is what a bare `import buelon`
    used to leave behind, and copying it would create an equally empty `.boo/` while
    reporting a migration that moved nothing.
    """
    try:
        with os.scandir(path) as entries:
            return any(True for _ in entries)
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return False


def migrate_bue_to_boo(
    old_path: str = OLD_DIR_PATH,
    new_path: str = NEW_DIR_PATH,
    verbose: bool = True,
) -> bool:
    """Copy `old_path` to `new_path` if and only if it is safe and needed.

    Returns True when a copy happened. A no-op -- returning False, printing nothing and
    creating nothing -- in every other case:

      * `BUELON_DIR_PATH` is set. The user has said where the state directory goes, so
        there is nothing to guess at and nothing to rename.
      * `BUELON_AUTO_MIGRATE=false`.
      * `new_path` already exists, as anything at all. Already migrated, or the name is
        taken; either way this must never overwrite it.
      * `old_path` is absent, is not a directory, or is empty.
    """
    if os.environ.get('BUELON_DIR_PATH') is not None:
        return False
    if not _auto_migrate_enabled():
        return False
    if os.path.exists(new_path):
        return False
    if not _has_contents(old_path):
        return False

    # Copy into a scratch directory and rename into place, so an interrupted or failing
    # copy cannot leave a half-populated `.boo/` behind -- which, existing, would block
    # every later attempt and be indistinguishable from a finished migration.
    staging = f'{new_path}.migrating.{os.getpid()}'
    shutil.rmtree(staging, ignore_errors=True)
    try:
        shutil.copytree(old_path, staging)
        if os.path.exists(new_path):  # lost a race with another process; it won.
            return False
        os.rename(staging, new_path)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    if verbose:
        print(
            f'buelon: copied {old_path}/ to {new_path}/ -- the state directory was '
            f'renamed ("boo", not "bue"). {old_path}/ is left in place and is no longer '
            f'read; delete it once you are happy.'
        )
    return True
