"""Marker making `buelon.static` a regular package (BUGS.md #26).

`web.py` reaches the bundled UI through `importlib.resources.files("buelon.static")`.
Without this file that is a *namespace* package, which `resources.files` only learned
to handle on Python 3.12 -- while setup.py declares `python_requires=">=3.10"`. It also
means `find_packages()` skips it, so the `package_data` entry keyed on `buelon.static`
never applies to a wheel.

The directory holds static assets, not code; nothing should be added here.
"""
