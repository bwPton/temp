# config/loader.py
import yaml

_config_instance = None  # module-level singleton

def init_config(path: str):
    global _config_instance
    if _config_instance is None:
        with open(path, "r") as f:
            _config_instance = yaml.safe_load(f)

def get_config():
    if _config_instance is None:
        raise RuntimeError("Config has not been initialized. Call init_config(path) first.")
    return _config_instance
    
def save_config(path):
    with open(path, 'w') as file:
        yaml.safe_dump(_config_instance, file)
        