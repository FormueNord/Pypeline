def function_constructor(func):
    """
    Decorator used to construct a function
    """
    def wrapper(*args, **kwargs):
        return lambda file_paths: func(file_paths, **kwargs)
    return wrapper


