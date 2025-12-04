fault_registry = {}

def register_fault(name):
    def deco(fn):
        fault_registry[str(name)] = fn
        return fn
    return deco
