import importlib as _il

from .base import apply_step

# Import submodules for side-effect registration (register_fault decorators)
for _m in ("disk", "network", "process", "watch", "session", "ephemeral"):
    _il.import_module(f"{__name__}.{_m}")

__all__ = ["apply_step"]
