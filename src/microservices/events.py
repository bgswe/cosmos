from datetime import datetime as dt
from typing import ClassVar, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from microservices.domain import Event, EventStream

"""Registration Events."""


class PointOfContact(BaseModel):
    """Nested model within Account for point of contact."""

    name: str
    email: str
    phone: str | None


class AccountRegistered(Event):
    """Signifies the creation of a new account in the system."""

    stream: ClassVar[EventStream] = EventStream.AccountRegistered

    business_name: str
    phone: str
    address: str
    point_of_contact: PointOfContact


class AccountUserRegistered(Event):
    """Signifies the creation of a new account user in the system."""

    stream: ClassVar[EventStream] = EventStream.AccountUserRegistered

    pk: UUID
    email: str
    first_name: str
    last_name: str

    roles: List[str]

    class Config:
        use_enum_values = True


"""Inspection Events."""


class CustomerModel(BaseModel):
    """Nested model within Inspection events."""

    pk: UUID
    name: str
    type: str
    email: str
    phone: Optional[str] = Field(...)


class InspectionCreated(Event):
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


class InvalidInspectionCancellation(Event):
    stream: ClassVar[EventStream] = EventStream.InspectionCancellationInvalid

    inspection_id: UUID
    cancellation_details: str


EVENTS = [
    AccountRegistered,
    AccountUserRegistered,
    InspectionCreated,
    InspectionUpdated,
    InspectionCancelled,
    InvalidInspectionCancellation,
]

STREAM_TO_EVENT_MAPPING = {e.stream: e for e in EVENTS}
