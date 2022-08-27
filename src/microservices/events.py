from datetime import datetime as dt
from typing import ClassVar, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from microservices.messages import Event, EventStream


"""Inspection Events."""


class CustomerModel(BaseModel):
    """..."""

    id: UUID
    name: str
    type: str
    email: str
    phone: Optional[str] = Field(...)


class InspectionNew(Event):
    """Event produced during add event of Inspections."""

    stream: ClassVar[EventStream] = EventStream.InspectionNew

    inspection_id: UUID
    property_address: str
    status: str  # InspectionStatus
    scheduled_datetime: Optional[dt] = Field(...)
    cancellation_details: Optional[str] = Field(...)

    account_id: UUID

    customer: CustomerModel


class InspectionUpdated(Event):
    """Event produced during update event of Inspections."""

    stream: ClassVar[EventStream] = EventStream.InspectionUpdated
    account_id: UUID

    inspection_id: UUID
    property_address: str
    status: str  # InspectionStatus
    scheduled_datetime: Optional[dt] = Field(...)
    cancellation_details: Optional[str] = Field(...)

    customer: CustomerModel

    # TODO: List of changed fields? Can use that on all update events


class InspectionCancelled(Event):
    """Event produced during the cancellation of a scheduled inspection."""

    stream: ClassVar[EventStream] = EventStream.InspectionCancelled

    inspection_id: UUID
    cancellation_details: str

    customer: CustomerModel


class InspectionInvalidCancellation(Event):
    stream: ClassVar[EventStream] = EventStream.InspectionCancellationInvalid

    inspection_id: UUID
    cancellation_details: str


EVENTS = [
    InspectionNew,
    InspectionUpdated,
    InspectionCancelled,
    InspectionInvalidCancellation,
]

STREAM_TO_EVENT_MAPPING = {e.stream: e for e in EVENTS}
