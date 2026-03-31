from uuid import uuid4


def new_event_id() -> str:
    return str(uuid4())


def build_instance_id(service_name: str, shard: int = 0) -> str:
    return f"{service_name}-{shard:02d}"
