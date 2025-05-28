
import functools

def singleton(cls):
    """
    Decorator to implement the Singleton design pattern.
    Ensures only one instance of the decorated class exists.

    Args:
        cls: The class to be decorated

    Returns:
        The singleton instance getter
    """
    instances = {}

    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance