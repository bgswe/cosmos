from collections import namedtuple
from datetime import datetime as dt
from enum import Enum
from typing import ClassVar, Optional

from pydantic import BaseModel, Field

from microservices.utils import uuid4

ConsumerConfig = namedtuple("ConsumerConfig", "name target retroactive")


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


class Event(BaseModel):
    """Base Event of our domain model."""

    # Each event has an associated named stream, essentially a type
    stream: ClassVar[EventStream]
    # Assign each event an ID for event tracking
    id: str = Field(default_factory=uuid4)

    class Config:
        use_enum_values = True

    @property
    def domain(self) -> str:
        return self.stream.value.split(".")[0]


"""Inspection Events."""


class CustomerModel(BaseModel):
    """..."""

    id: str
    name: str
    type: str
    email: str
    phone: Optional[str] = Field(...)


class InspectionNew(Event):
    """Event produced during add event of Inspections."""

    stream: ClassVar[EventStream] = EventStream.InspectionNew

    inspection_id: str
    property_address: str
    status: str  # InspectionStatus
    scheduled_datetime: Optional[dt] = Field(...)
    cancellation_details: Optional[str] = Field(...)

    organization_id: str

    customer: CustomerModel


class InspectionUpdated(Event):
    """Event produced during update event of Inspections."""

    stream: ClassVar[EventStream] = EventStream.InspectionUpdated

    inspection_id: str
    property_address: str
    status: str  # InspectionStatus
    scheduled_datetime: Optional[dt] = Field(...)
    cancellation_details: Optional[str] = Field(...)

    organization_id: str

    customer: CustomerModel

    # TODO: List of changed fields? Can use that on all update events


class InspectionCancelled(Event):
    """Event produced during the cancellation of a scheduled inspection."""

    stream: ClassVar[EventStream] = EventStream.InspectionCancelled

    inspection_id: str
    cancellation_details: str

    customer: CustomerModel


class InspectionInvalidCancellation(Event):
    stream: ClassVar[EventStream] = EventStream.InspectionCancellationInvalid

    inspection_id: str
    cancellation_details: str


EVENTS = [
    InspectionNew,
    InspectionUpdated,
    InspectionCancelled,
    InspectionInvalidCancellation,
]

STREAM_TO_EVENT_MAPPING = {e.stream: e for e in EVENTS}
