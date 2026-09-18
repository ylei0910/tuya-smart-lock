"""Diagnostic sensors for Tuya Smart Lock.

Datapoints and their types/enum ranges below are taken from the device's
own Tuya specification (/v1.0/iot-03/devices/{id}/specification), not
guessed from field names - see lock.py for why that distinction matters
for this device.
"""

import logging

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import PERCENTAGE, EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_DEVICE_ID, CONF_DEVICE_NAME, DOMAIN

_LOGGER = logging.getLogger(__name__)

DESCRIPTIONS: tuple[SensorEntityDescription, ...] = (
    SensorEntityDescription(
        key="residual_electricity",
        translation_key="battery",
        name="Battery",
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
    SensorEntityDescription(
        key="alarm_lock",
        translation_key="alarm",
        name="Alarm",
        device_class=SensorDeviceClass.ENUM,
        options=[
            "wrong_finger",
            "wrong_password",
            "wrong_card",
            "wrong_face",
            "tongue_bad",
            "tongue_not_out",
            "unclosed_time",
            "key_in",
            "too_hot",
            "low_battery",
        ],
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
    SensorEntityDescription(
        key="link_mode",
        translation_key="connection_mode",
        name="Connection Mode",
        device_class=SensorDeviceClass.ENUM,
        options=["keep", "sleep"],
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
    SensorEntityDescription(
        key="closed_opened",
        translation_key="closed_opened_raw",
        name="Closed/Open Raw Value",
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up diagnostic sensors from a config entry."""
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator = data["coordinator"]
    entry_data = data["entry_data"]
    device_id = entry_data[CONF_DEVICE_ID]
    device_name = entry_data[CONF_DEVICE_NAME]

    async_add_entities(
        TuyaSmartLockSensor(coordinator, device_id, device_name, description)
        for description in DESCRIPTIONS
    )


class TuyaSmartLockSensor(CoordinatorEntity, SensorEntity):
    """Reads a single datapoint from the shared status coordinator."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device_id: str, device_name: str, description) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._device_id = device_id
        self._device_name = device_name
        self._attr_unique_id = f"tuya_smart_lock_{device_id}_{description.key}"

    @property
    def device_info(self):
        """Link to the same device as the lock entity."""
        return {
            "identifiers": {("tuya", self._device_id)},
            "name": self._device_name,
            "manufacturer": "Tuya",
        }

    @property
    def available(self) -> bool:
        return (
            self.coordinator.last_update_success
            and self.coordinator.data is not None
            and self.entity_description.key in self.coordinator.data
        )

    @property
    def native_value(self):
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.get(self.entity_description.key)
