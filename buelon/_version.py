"""Single source of truth for buelon's version (BUGS.md #26).

The literal lives in `version.json` at the repo root -- that is the one file to
edit for a release. `setup.py` reads it at build time and stamps it into the
distribution's metadata.

At runtime `version.json` is preferred when it is there: a checkout (and an
editable install) has it, and it is the file a release actually bumps, so it
beats a `.egg-info` that may have been generated several versions ago. An
ordinary install has no such file next to the package and falls through to the
distribution metadata, which setup.py filled in from this same literal. Note
that metadata normalises the string ("1.0.78-alpha1" -> "1.0.78a1").
"""
import json
import os
from importlib import metadata

_VERSION_JSON = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'version.json')


def get_version() -> str:
    try:
        with open(_VERSION_JSON, 'r', encoding='utf-8') as f:
            return json.load(f)['last']
    except (OSError, ValueError, KeyError):
        pass

    try:
        return metadata.version('buelon')
    except metadata.PackageNotFoundError:
        return 'unknown'


__version__ = get_version()
