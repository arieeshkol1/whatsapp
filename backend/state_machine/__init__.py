################################################################################
# !!! IMPORTANT !!!
#  This __init__.py allows to load the relevant classes from the State Machine.
#  By importing this file, we leverage "globals" and "getattr" to dynamically
#  execute the Step Function's inner Lambda Functions classes.
################################################################################

from __future__ import annotations

import sys
import types


def _ensure_packaging_licenses() -> None:
    """Provide packaging.licenses when the installed distribution omits it."""

    try:  # pragma: no branch - happy path exits immediately
        import packaging.licenses  # type: ignore  # noqa: F401

        return
    except ModuleNotFoundError:  # pragma: no cover - environment dependent guard
        pass

    module = types.ModuleType("packaging.licenses")
    module.__all__ = []
    sys.modules[module.__name__] = module


_ensure_packaging_licenses()

try:  # pragma: no cover - poetry is not present in production Lambdas
    from poetry.plugins.application_plugin import ApplicationPlugin
except ModuleNotFoundError:  # pragma: no cover - runtime without poetry
    ApplicationPlugin = None  # type: ignore[assignment]
else:

    class PackagingLicensesPlugin(ApplicationPlugin):
        """Poetry plugin hook to ensure packaging.licenses availability."""

        def activate(self, application) -> None:  # pragma: no cover - CLI hook
            _ensure_packaging_licenses()


# Validation
from state_machine.processing.validate_message import ValidateMessage  # noqa

# Processing
from state_machine.processing.process_text import ProcessText  # noqa
from state_machine.processing.process_voice import ProcessVoice  # noqa
from state_machine.processing.send_message import SendMessage  # noqa

# Utils
from state_machine.utils.success import Success  # noqa
from state_machine.utils.failure import Failure  # noqa
