import uuid


def uuid4() -> str:
    """Reduces uuid lib uuid4 generation to str type."""
    return str(uuid.uuid4())
