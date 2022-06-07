import uuid

def uuid4() -> str:
    return str(uuid.uuid4().hex)
