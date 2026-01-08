from .base import apply_step
import importlib as _il

# Import submodules for side-effect registration (register_fault decorators)
for _m in ("disk", "network", "process"):
    _il.import_module(f"{__name__}.{_m}")

__all__ = ["apply_step"]
