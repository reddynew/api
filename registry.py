
registry = {}

def register(task_type):
    """Decorator to register a handler."""
    def decorator(handlerfunc):
        registry[task_type] = handlerfunc
        return handlerfunc
    return decorator

def retrieve(task_type):
    """Retrieve the handler for a given task type."""
    handler = registry.get(task_type)
    if not handler:
        raise ValueError(f"No handler registered for task type: {task_type}")
    return handler
