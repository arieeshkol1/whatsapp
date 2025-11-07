import json
import importlib
import traceback

# Own imports
from .__init__ import *  # noqa NOSONAR

logger = custom_logger()

CLASS_MODULE_MAP = {
    "ValidateMessage": "backend.state_machine.utils.validate_message",
    "ProcessText": "backend.state_machine.processing.process_text",
    "ProcessVoice": "backend.state_machine.processing.process_voice",
    "SendMessage": "backend.state_machine.processing.send_message",
    "Success": "backend.state_machine.utils.success",
    "Failure": "backend.state_machine.utils.failure",
}


def _extract_params_and_event(raw_event: dict):
    params = (raw_event or {}).get("params") or {}
    class_name = params.get("class_name")
    method_name = params.get("method_name")

    if isinstance(raw_event, dict) and "event" in raw_event:
        inner_event = raw_event.get("event")
    else:
        inner_event = raw_event

    return class_name, method_name, inner_event


def _base_module_names_for_class(class_name: str) -> list[str]:
    base_modules: list[str] = []
    if class_name in CLASS_MODULE_MAP:
        base_modules.append(CLASS_MODULE_MAP[class_name])
    else:
        snake = [class_name[0].lower()]
        for char in class_name[1:]:
            if char.isupper():
                snake.append("_")
                snake.append(char.lower())
            else:
                snake.append(char)
        module_stub = "".join(snake)
        base_modules.append(f"state_machine.processing.{module_stub}")
        base_modules.append(f"state_machine.utils.{module_stub}")
    return base_modules


def _candidate_modules_for_class(class_name: str) -> list[str]:
    base_modules = _base_module_names_for_class(class_name)
    candidates: list[str] = []
    for module_name in base_modules:
        if module_name not in candidates:
            candidates.append(module_name)
        if not module_name.startswith("backend."):
            backend_prefixed = f"backend.{module_name}"
            if backend_prefixed not in candidates:
                candidates.append(backend_prefixed)
    return candidates


def _resolve_target(class_name: str, method_name: str):
    if not class_name:
        raise ValueError("Missing params.class_name")
    if not method_name:
        raise ValueError("Missing params.method_name")

    last_exc: ModuleNotFoundError | None = None
    for module_name in _candidate_modules_for_class(class_name):
        try:
            module = importlib.import_module(module_name)
            break
        except ModuleNotFoundError as exc:
            last_exc = exc
    else:
        raise ModuleNotFoundError(
            f"Could not resolve module for class '{class_name}'"
        ) from last_exc

    clazz = getattr(module, class_name, None)
    if clazz is None:
        raise ImportError(
            f"Class '{class_name}' not found in module '{module.__name__}'"
        )

    target = getattr(clazz, method_name, None)
    if target is None or not callable(target):
        raise AttributeError(
            f"Method '{method_name}' not found/callable on class '{class_name}'"
        )

    return clazz


def lambda_handler(event, context):
    logger.info("Lambda Main Handler Event")
    logger.info(event)

    try:
        class_name, method_name, inner_event = _extract_params_and_event(event)
        clazz = _resolve_target(class_name, method_name)

        instance = clazz(inner_event)
        logger.debug(f"dynamically loaded target_instance: {instance}")

        target_method = getattr(instance, method_name)
        logger.debug(f"dynamically loaded target_method: {target_method}")

        result = target_method()
        return result

    except Exception as exc:
        logger.error(f"Error while executing lambda handler: {exc}")
        try:
            logger.error(f"Lambda Initial Event was: {json.dumps(event)}")
        except Exception:
            logger.error("Lambda Initial Event (non-serializable)")

        try:
            _, _, inner_event = _extract_params_and_event(event or {})
            logger.error(f"Lambda Main Event was: {json.dumps(inner_event)}")
        except Exception:
            logger.error("Lambda Main Event (non-serializable)")

        logger.error(traceback.format_exc())
        raise
