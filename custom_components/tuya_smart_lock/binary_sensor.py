"""Diagnostic binary sensors for Tuya Smart Lock.

Only exposes datapoints confirmed via the device's own Tuya specification
(/v1.0/iot-03/devices/{id}/specification) to be Boolean-typed. There is no
generic door-open/closed datapoint on this device category - see lock.py
and README for details on closed_opened, which is not a clean boolean.
"""

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_DEVICE_ID, CONF_DEVICE_NAME, DOMAIN

_LOGGER = logging.getLogger(__name__)

DESCRIPTIONS: tuple[BinarySensorEntityDescription, ...] = (
    BinarySensorEntityDescription(
        key="open_inside",
        translation_key="open_inside",
        name="Opened From Inside",
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
    BinarySensorEntityDescription(
        key="doorbell",
        translation_key="doorbell",
        name="Doorbell",
    ),
    BinarySensorEntityDescription(
        key="hijack",
        translation_key="hijack",
        name="Hijack Alarm",
        device_class=BinarySensorDeviceClass.SAFETY,
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up diagnostic binary sensors from a config entry."""
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator = data["coordinator"]
    entry_data = data["entry_data"]
    device_id = entry_data[CONF_DEVICE_ID]
    device_name = entry_data[CONF_DEVICE_NAME]

    async_add_entities(
        TuyaSmartLockBinarySensor(coordinator, device_id, device_name, description)
        for description in DESCRIPTIONS
    )


class TuyaSmartLockBinarySensor(CoordinatorEntity, BinarySensorEntity):
    """Reads a single Boolean datapoint from the shared status coordinator."""

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
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.get(self.entity_description.key)
