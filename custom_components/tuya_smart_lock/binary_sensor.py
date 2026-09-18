"""Binary sensor entity for Tuya Smart Lock door contact."""

import logging
from datetime import timedelta

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_DEVICE_ID, CONF_DEVICE_NAME, DOMAIN

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = timedelta(seconds=60)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the door sensor from a config entry."""
    data = hass.data[DOMAIN][entry.entry_id]
    api = data["api"]
    entry_data = data["entry_data"]
    device_id = entry_data[CONF_DEVICE_ID]
    device_name = entry_data[CONF_DEVICE_NAME]

    async_add_entities(
        [TuyaSmartLockDoorSensor(api, device_id, device_name)],
        True,
    )


class TuyaSmartLockDoorSensor(BinarySensorEntity):
    """Reports door open/closed state via the closed_opened datapoint."""

    _attr_has_entity_name = True
    _attr_name = "Door"
    _attr_device_class = BinarySensorDeviceClass.DOOR
    _attr_should_poll = True

    def __init__(self, api, device_id: str, device_name: str) -> None:
        self._api = api
        self._device_id = device_id
        self._attr_unique_id = f"tuya_smart_lock_{device_id}_door"
        self._attr_available = False
        self._attr_is_on = None
        self._device_name = device_name

    @property
    def device_info(self):
        """Link to the same device as the lock entity."""
        return {
            "identifiers": {("tuya", self._device_id)},
            "name": self._device_name,
            "manufacturer": "Tuya",
        }

    async def async_update(self) -> None:
        """Refresh door open/closed state from Tuya."""
        state = await self._api.async_get_door_state(self._device_id)
        if state is None:
            return
        self._attr_is_on = state
        self._attr_available = True
