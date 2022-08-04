from collections import namedtuple
from datetime import datetime as dt
from enum import Enum
from typing import ClassVar, Optional

from pydantic import BaseModel, Field

ConsumerConfig = namedtuple("ConsumerConfig", "name target retroactive")


class Domain(Enum):
    Registration = "registration"
    Inspection = "inspection"


class EventStream(Enum):
    RegistrationNew = f"{Domain.Registration.value}.new"

    InspectionNew = f"{Domain.Inspection.value}.new"
    InspectionUpdated = f"{Domain.Inspection.value}.updated"
    InspectionCancelled = f"{Domain.Inspection.value}.cancelled"
    InspectionCancellationInvalid = f"{Domain.Inspection.value}.cancellation_invalid"


class Event(BaseModel):
    """Base Event of our domain model."""

    stream: ClassVar[EventStream]

    class Config:
        use_enum_values = True

    @property
    def domain(self) -> str:
        return self.stream.value.split(".")[0]


"""Registration Events."""


class RegistrationNew(Event):
    """..."""

    stream: ClassVar[EventStream] = EventStream.RegistrationNew

    organization_id: str
    organization_name: str
    organization_phone: str
    organization_address: str


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
    reason: str


EVENTS = [
    RegistrationNew,
]

STREAM_TO_EVENT_MAPPING = {e.stream: e for e in EVENTS}
