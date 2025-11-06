import json
import importlib
import traceback
from typing import Tuple

from common.logger import custom_logger

logger = custom_logger()

# Resolve to the backend.state_machine package to avoid collisions
CLASS_MODULE_MAP = {
    "SendMessage": "backend.state_machine.processing.send_message",
    "ProcessText": "backend.state_machine.processing.process_text",
    "ProcessVoice": "backend.state_machine.processing.process_voice",
    "ValidateMessage": "backend.state_machine.processing.validate_message",
    "Success": "backend.state_machine.utils.success",
    "Failure": "backend.state_machine.utils.failure",
}


def _camel_to_snake(name: str) -> str:
    pieces = []
    for index, char in enumerate(name):
        if char.isupper() and index != 0:
            pieces.append("_")
        pieces.append(char.lower())
    return "".join(pieces)


def _extract_params_and_event(raw_event: dict) -> Tuple[str, str, dict]:
    params = (raw_event or {}).get("params") or {}
    class_name = params.get("class_name")
    method_name = params.get("method_name")
    inner_event = (
        raw_event.get("event")
        if isinstance(raw_event, dict) and "event" in raw_event
        else raw_event
    )
    return class_name, method_name, inner_event


def _resolve_target(class_name: str, method_name: str):
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
                f"backend.state_machine.processing.{snake_name}",
                f"backend.state_machine.utils.{snake_name}",
            )
        )

    module = None
    last_exception = None
    for candidate in candidate_modules:
        try:
            module = importlib.import_module(candidate)
            module_name = candidate
            break
        except ModuleNotFoundError as exc:
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
    logger.info("Lambda Main Handler Event", extra={"event": event})

    try:
        class_name, method_name, inner_event = _extract_params_and_event(event)
        clazz, _ = _resolve_target(class_name, method_name)

        target_instance = clazz(inner_event)
        logger.debug("Dynamically loaded target instance", extra={"class": class_name})

        target_method = getattr(target_instance, method_name)
        logger.debug(
            "Dynamically resolved target method", extra={"method": method_name}
        )

        return target_method()

    except Exception as e:
        logger.error("Error while executing lambda handler", extra={"error": str(e)})
        try:
            logger.error("Lambda Initial Event (raw)", extra={"event": event})
        except Exception:
            logger.error("Lambda Initial Event (non-serializable)")

        try:
            _, _, inner_event = _extract_params_and_event(event or {})
            logger.error("Lambda Main Event (inner)", extra={"event": inner_event})
        except Exception:
            logger.error("Lambda Main Event (non-serializable)")

        logger.error(traceback.format_exc())
        raise
