# The `.bue/` -> `.boo/` state-directory migration has to happen before anything else in
# the package. `settings` resolves `DIR_PATH` at import time, so by the time any other
# submodule is loaded the decision has already been made. `migration` imports nothing from
# buelon, which is what makes it safe to run here. BUGS.md #55.
from . import migration

migration.migrate_bue_to_boo()

# Import modules from subpackages
from . import settings
from .core import action, execution, loop, pipe, pipe_interpreter, step, step_definition
from . import bucket, hub, worker, command_line
# `c_*` names are kept for backwards compatibility. The Cython build was removed
# (see BUGS.md #25); they are plain aliases for the pure-Python modules.
from . import bucket as c_bucket, hub as c_hub, worker as c_worker
from .helpers import json_parser, pipe_util, postgres, sqlite3_helper
from .examples import demo, example


# Define the public API
__all__ = ['bucket', 'hub', 'worker', 'command_line', 'core', 'helpers', 'examples']
