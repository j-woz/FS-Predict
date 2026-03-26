import os

import yaml


DEFAULT_SETTINGS_PATH = "./settings.yaml"
SECTION_NAMES = {"common", "server", "client", "make_obs_fut"}


def load_settings(path=None, required=False):
    settings_path = path or DEFAULT_SETTINGS_PATH

    if not os.path.exists(settings_path):
        if required:
            raise FileNotFoundError(f"settings file not found: '{settings_path}'")
        return {}

    with open(settings_path, "r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"settings file must contain a YAML mapping: '{settings_path}'")

    return data


def program_settings(path, program_name, required=False):
    data = load_settings(path=path, required=required)

    root_defaults = {
        key: value
        for key, value in data.items()
        if key not in SECTION_NAMES
    }

    common = data.get("common", {})
    if common is None:
        common = {}
    if not isinstance(common, dict):
        raise ValueError("'common' settings must be a mapping")

    section = data.get(program_name, {})
    if section is None:
        section = {}
    if not isinstance(section, dict):
        raise ValueError(f"'{program_name}' settings must be a mapping")

    merged = {}
    merged.update(root_defaults)
    merged.update(common)
    merged.update(section)
    return merged


def merge_keyvalue_settings(config, cli_keyvals=None, reserved_keys=None):
    reserved = set(reserved_keys or [])
    merged = {}

    config_keyvalues = config.get("keyvalues", {})
    if config_keyvalues is not None:
        if not isinstance(config_keyvalues, dict):
            raise ValueError("'keyvalues' settings must be a mapping")
        for key, value in config_keyvalues.items():
            merged[str(key)] = "" if value is None else str(value)

    for key, value in config.items():
        if key in reserved or key == "keyvalues":
            continue
        if isinstance(value, (dict, list)):
            continue
        merged[str(key)] = "" if value is None else str(value)

    for key, value in parse_keyvals(cli_keyvals).items():
        merged[key] = value

    return merged


def parse_keyvals(keyvals):
    result = {}
    for kv in keyvals or []:
        if "=" not in kv:
            raise ValueError(f"bad keyvalue pair: '{kv}'")
        key, value = kv.split("=", 1)
        result[key] = value
    return result


def to_keyval_list(settings):
    return [f"{key}={value}" for key, value in settings.items()]
