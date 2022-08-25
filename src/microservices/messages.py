from enum import Enum
from typing import ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Domain(Enum):
    """The registered domains of the system."""

    AccountManagement = "account_management"
    Inspection = "inspection"


class EventStream(Enum):
    """The registered event streams in the system."""

    InspectionNew = f"{Domain.Inspection.value}.new"
    InspectionUpdated = f"{Domain.Inspection.value}.updated"
    InspectionCancelled = f"{Domain.Inspection.value}.cancelled"
    InspectionCancellationInvalid = f"{Domain.Inspection.value}.cancellation_invalid"

    # NOTE: These exist for testing purposes only
    # TODO: Try to figure out another way to have mock streams, w/o
    # changing source. Tried subclassing EventStream, nogo. :(
    MockA = "domain.stream_a"
    MockB = "domain.stream_b"
    MockC = "domain.stream_c"


class Message(BaseModel):
    """Base Message model/schema."""

    id: UUID = Field(default_factory=uuid4)

    class Config:
        use_enum_values = True


class Event(Message):
    """Base Event of our domain model."""

    # Each event has an associated named stream, essentially a type
    stream: ClassVar[EventStream]

    @property
    def domain(self) -> str:
        return self.stream.value.split(".")[0]


class Command(Message):
    """Base Command message type created for extension."""

    client_id: UUID
