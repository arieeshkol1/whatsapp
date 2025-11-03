import json
import importlib
import traceback

from common.logger import custom_logger

logger = custom_logger()

# Map known classes to their modules for reliable import.
# Add here if you create more processor classes that live outside the
# conventional camelCase-to-snake_case lookup.
CLASS_MODULE_MAP = {
    "SendMessage": "state_machine.processing.send_message",
    "ProcessText": "state_machine.processing.process_text",
    "ProcessVoice": "state_machine.processing.process_voice",
    "ValidateMessage": "state_machine.processing.validate_message",
    "Success": "state_machine.utils.success",
    "Failure": "state_machine.utils.failure",
}


def _camel_to_snake(name: str) -> str:
    pieces = []
    for index, char in enumerate(name):
        if char.isupper() and index != 0:
            pieces.append("_")
        pieces.append(char.lower())
    return "".join(pieces)


def _extract_params_and_event(raw_event: dict):
    """
    Accepts either:
      A) {"params":{"class_name":"SendMessage","method_name":"send_message"}, "event":{...}}
      B) Any other payload (we'll raise if params are missing).
    Returns: (class_name, method_name, inner_event)
    """
    params = (raw_event or {}).get("params") or {}
    class_name = params.get("class_name")
    method_name = params.get("method_name")

    # Inner event (business payload) may be under "event" (wrapper) or the raw event itself.
    inner_event = (
        raw_event.get("event")
        if isinstance(raw_event, dict) and "event" in raw_event
        else raw_event
    )

    return class_name, method_name, inner_event


def _resolve_target(class_name: str, method_name: str):
    """
    Dynamically import the module and resolve the class + method.
    """
    if not class_name:
        raise ValueError("Missing params.class_name")
    if not method_name:
        raise ValueError("Missing params.method_name")

    module_name = CLASS_MODULE_MAP.get(class_name)
    candidate_modules = []
    if module_name:
        candidate_modules.append(module_name)
    else:
        snake_name = _camel_to_snake(class_name)
        candidate_modules.extend(
            (
                f"state_machine.processing.{snake_name}",
                f"state_machine.utils.{snake_name}",
            )
        )

    module = None
    last_exception = None
    for candidate in candidate_modules:
        try:
            module = importlib.import_module(candidate)
            module_name = candidate
            break
        except ModuleNotFoundError as exc:  # pragma: no cover - handled by fallback
            last_exception = exc
    if module is None:
        raise ModuleNotFoundError(
            f"Could not resolve module for class '{class_name}'"
        ) from last_exception

    clazz = getattr(module, class_name, None)
    if clazz is None:
        raise ImportError(f"Class '{class_name}' not found in module '{module_name}'")

    target = getattr(clazz, method_name, None)
    if target is None or not callable(target):
        raise AttributeError(
            f"Method '{method_name}' not found/callable on class '{class_name}'"
        )

    return clazz, method_name


def lambda_handler(event, context):
    """
    Main AWS Lambda handler.
    Expects:
      event.params.class_name    (e.g., "SendMessage")
      event.params.method_name   (e.g., "send_message")
      event.event                (the inner event payload your class expects)
    """
    logger.info("Lambda Main Handler Event")
    logger.info(event)

    try:
        class_name, method_name, inner_event = _extract_params_and_event(event)

        clazz, _ = _resolve_target(class_name, method_name)

        # Instantiate the target class with the inner event (your classes expect `event` in __init__).
        target_instance = clazz(inner_event)
        logger.debug(f"dynamically loaded target_instance: {target_instance}")

        target_method = getattr(target_instance, method_name)
        logger.debug(f"dynamically loaded target_method: {target_method}")

        # Execute and return result (your methods typically return an event dict).
        result = target_method()
        return result

    except Exception as e:
        # Mirror your existing structured error logs to help debugging in CloudWatch.
        logger.error(
            f"Error while executing lambda handler: {e}",
        )
        try:
            logger.error(f"Lambda Initial Event was: {json.dumps(event)}")
        except Exception:
            logger.error("Lambda Initial Event (non-serializable)")

        # If inner event was present, log it too.
        try:
            _, _, inner_event = _extract_params_and_event(event or {})
            logger.error(f"Lambda Main Event was: {json.dumps(inner_event)}")
        except Exception:
            logger.error("Lambda Main Event (non-serializable)")

        logger.error(traceback.format_exc())
        # Re-raise so Step Functions / caller gets a failure.
        raise
