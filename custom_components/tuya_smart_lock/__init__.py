"""Tuya Smart Lock integration."""

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import CONF_ACCESS_ID, CONF_ACCESS_SECRET, CONF_API_REGION, CONF_DEVICE_ID, DOMAIN
from .tuya_api import TuyaCloudApi

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.LOCK, Platform.SENSOR, Platform.BINARY_SENSOR]

# The lock entity itself is deliberately not coordinator-driven (it's
# on-demand: fetches on load and after each lock/unlock's verification
# window). This coordinator is only for the read-only diagnostic
# entities (battery, alarm, doorbell, etc.) added in sensor.py /
# binary_sensor.py, so they share one poll instead of one API call each.
STATUS_SCAN_INTERVAL = timedelta(seconds=60)


class TuyaSmartLockStatusCoordinator(DataUpdateCoordinator[dict]):
    """Polls the device status endpoint once and fans it out to diagnostic entities."""

    def __init__(self, hass: HomeAssistant, api: TuyaCloudApi, device_id: str) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_status",
            update_interval=STATUS_SCAN_INTERVAL,
        )
        self._api = api
        self._device_id = device_id

    async def _async_update_data(self) -> dict:
        dps = await self._api.async_get_status(self._device_id)
        if dps is None:
            raise UpdateFailed("Failed to fetch device status")
        return dps


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Tuya Smart Lock from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    api = TuyaCloudApi(
        access_id=entry.data[CONF_ACCESS_ID],
        access_secret=entry.data[CONF_ACCESS_SECRET],
        region=entry.data[CONF_API_REGION],
    )
    device_id = entry.data[CONF_DEVICE_ID]

    coordinator = TuyaSmartLockStatusCoordinator(hass, api, device_id)
    # async_refresh() (not async_config_entry_first_refresh()) deliberately:
    # a failed status fetch should only leave the diagnostic entities
    # unavailable, not block the whole config entry - especially not the
    # lock entity, which has its own independent, more important recovery
    # path and shouldn't be coupled to this coordinator's health.
    await coordinator.async_refresh()

    hass.data[DOMAIN][entry.entry_id] = {
        "api": api,
        "entry_data": entry.data,
        "coordinator": coordinator,
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
