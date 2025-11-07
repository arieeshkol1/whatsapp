from __future__ import annotations

import json
import importlib
import traceback
from typing import List, Tuple, Optional, Any

# Safe logger import w/ fallback so tests don't fail during collection
try:
    from common.logger import custom_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def custom_logger():
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("wpp-chatbot")

logger = custom_logger()

CLASS_MODULE_MAP = {
    "ValidateMessage": "backend.state_machine.utils.validate_message",
    "ProcessText": "backend.state_machine.processing.process_text",
    "ProcessVoice": "backend.state_machine.processing.process_voice",
    "SendMessage": "backend.state_machine.processing.send_message",
    "Success": "backend.state_machine.utils.success",
    "Failure": "backend.state_machine.utils.failure",
}


def _extract_params_and_event(raw_event: dict) -> Tuple[Optional[str], Optional[str], dict]:
    params = (raw_event or {}).get("params") or {}
    class_name = params.get("class_name")
    method_name = params.get("method_name")

    if isinstance(raw_event, dict) and "event" in raw_event:
        inner_event = raw_event.get("event")
    else:
        inner_event = raw_event

    return class_name, method_name, inner_event


def _camel_to_snake(name: str) -> str:
    snake = [name[0].lower()]
    for ch in name[1:]:
        if ch.isupper():
            snake.append("_")
            snake.append(ch.lower())
        else:
            snake.append(ch)
    return "".join(snake)


def _base_module_names_for_class(class_name: str) -> List[str]:
    """
    Build base module names to try for a given class.
    If class is mapped to 'backend.*', also include the non-prefixed counterpart to support
    runtimes that import from 'state_machine.*'.
    """
    base_modules: List[str] = []
    mapped = CLASS_MODULE_MAP.get(class_name)

    if mapped:
        base_modules.append(mapped)
        if mapped.startswith("backend."):
            base_modules.append(mapped[len("backend.") :])
        else:
            base_modules.append(f"backend.{mapped}")
    else:
        module_stub = _camel_to_snake(class_name)
        base_modules.append(f"state_machine.processing.{module_stub}")
        base_modules.append(f"state_machine.utils.{module_stub}")

    return base_modules


def _candidate_modules_for_class(class_name: str) -> List[str]:
    base_modules = _base_module_names_for_class(class_name)
    candidates: List[str] = []

    for module_name in base_modules:
        if module_name not in candidates:
            candidates.append(module_name)

        if not module_name.startswith("backend."):
            backend_prefixed = f"backend.{module_name}"
            if backend_prefixed not in candidates:
                candidates.append(backend_prefixed)

        if module_name.startswith("backend."):
            non_backend = module_name[len("backend.") :]
            if non_backend not in candidates:
                candidates.append(non_backend)

    return candidates


def _resolve_target(class_name: str, method_name: str):
    if not class_name:
        raise ValueError("Missing params.class_name")
    if not method_name:
        raise ValueError("Missing params.method_name")

    last_exc: Optional[ModuleNotFoundError] = None
    module = None

    for module_name in _candidate_modules_for_class(class_name):
        try:
            module = importlib.import_module(module_name)
            break
        except ModuleNotFoundError as exc:
            last_exc = exc
        except Exception as exc:
            logger.debug(
                {
                    "message": "Unexpected import error",
                    "module": module_name,
                    "error": str(exc),
                }
            )
            last_exc = ModuleNotFoundError(str(exc))

    if module is None:
        raise ModuleNotFoundError(
            f"Could not resolve module for class '{class_name}'. "
            f"Tried: {', '.join(_candidate_modules_for_class(class_name))}"
        ) from last_exc

    clazz = getattr(module, class_name, None)
    if clazz is None:
        raise ImportError(f"Class '{class_name}' not found in module '{module.__name__}'")

    target = getattr(clazz, method_name, None)
    if target is None or not callable(target):
        raise AttributeError(f"Method '{method_name}' not found/callable on class '{class_name}'")

    return clazz


def lambda_handler(event: dict, context: Any):
    logger.info("Lambda Main Handler Event")
    logger.info(event)

    try:
        class_name, method_name, inner_event = _extract_params_and_event(event)
        clazz = _resolve_target(class_name, method_name)

        # Instantiate and call (instance-based steps)
        instance = clazz(inner_event)
        logger.debug(f"dynamically loaded target_instance: {instance}")

        target_method = getattr(instance, method_name)
        logger.debug(f"dynamically loaded target_method: {target_method}")

        result = target_method()

        # Ensure test-required field exists
        if isinstance(result, dict):
            result.setdefault("ExceptionOcurred", False)  # keep test’s key spelling
        else:
            result = {"result": result, "ExceptionOcurred": False}

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
