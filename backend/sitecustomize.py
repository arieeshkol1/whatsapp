"""Lambda bootstrap helper to alias the package namespace.

AWS Lambda packages this project by zipping the *contents* of the
``backend`` directory. As a result, the deployed artifact exposes modules
like ``state_machine`` and ``common`` at the root of ``/var/task`` rather
than under a ``backend`` package. The existing Lambda configuration,
however, imports the handler as ``backend.state_machine.state_machine_handler``.

To remain compatible with that configuration we expose a lightweight
``backend`` namespace when the real package is absent.  The logic executes
via the standard ``sitecustomize`` hook so it runs before the runtime loads
any handler modules.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

# If the real ``backend`` package exists (e.g. during local development), we
# leave it untouched. In the packaged Lambda artifact the module is missing,
# so we create a namespace module whose ``__path__`` points at the artifact
# root.  That allows imports such as ``backend.state_machine`` to succeed even
# though the files live directly under ``/var/task``.
if (
    importlib.util.find_spec("backend") is None
):  # pragma: no cover - only true in Lambda
    module = types.ModuleType("backend")
    module.__path__ = [str(Path(__file__).resolve().parent)]  # type: ignore[attr-defined]
    sys.modules["backend"] = module
