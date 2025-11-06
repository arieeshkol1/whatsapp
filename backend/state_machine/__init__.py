################################################################################
# !!! IMPORTANT !!!
#  This __init__.py allows to load the relevant classes from the State Machine.
#  By importing this file, we leverage "globals" and "getattr" to dynamically
#  execute the Step Function's inner Lambda Functions classes.
################################################################################

import os

if os.environ.get("STATE_MACHINE_IMPORT_MODE") == "minimal":
    # Minimal import surface for unit tests that do not require AWS clients.
    from .processing.process_text import ProcessText  # noqa
else:
    # Validation
    from .utils.validate_message import ValidateMessage  # noqa

    # Processing
    from .processing.process_text import ProcessText  # noqa
    from .processing.process_voice import ProcessVoice  # noqa
    from .processing.send_message import SendMessage  # noqa

    # Utils
    from .utils.success import Success  # noqa
    from .utils.failure import Failure  # noqa
