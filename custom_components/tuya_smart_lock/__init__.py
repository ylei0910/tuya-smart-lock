"""Tuya Smart Lock integration."""

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_call_later
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
STATUS_POLL_INTERVAL_SECONDS = 60


class TuyaSmartLockStatusCoordinator(DataUpdateCoordinator[dict]):
    """Polls the device status endpoint and fans it out to diagnostic entities.

    Deliberately does NOT use DataUpdateCoordinator's own update_interval
    scheduling (passing update_interval=... to super().__init__) - in
    practice that stopped firing after the first cycle. Instead this
    self-reschedules via async_call_later, the same mechanism lock.py's
    _schedule_verification uses, which has been reliable in testing.
    """

    def __init__(self, hass: HomeAssistant, api: TuyaCloudApi, device_id: str) -> None:
        super().__init__(hass, _LOGGER, name=f"{DOMAIN}_status")
        self._api = api
        self._device_id = device_id
        self._cancel_next_poll = None

    async def _async_update_data(self) -> dict:
        dps = await self._api.async_get_status(self._device_id)
        if dps is None:
            raise UpdateFailed("Failed to fetch device status")
        return dps

    def start_polling(self) -> None:
        """Begin the self-rescheduling poll loop."""
        self._schedule_next_poll()

    def stop_polling(self) -> None:
        """Cancel any pending scheduled poll."""
        if self._cancel_next_poll is not None:
            self._cancel_next_poll()
            self._cancel_next_poll = None

    def _schedule_next_poll(self) -> None:
        self._cancel_next_poll = async_call_later(
            self.hass, STATUS_POLL_INTERVAL_SECONDS, self._async_poll_once
        )

    async def _async_poll_once(self, _now) -> None:
        self._cancel_next_poll = None
        await self.async_refresh()
        self._schedule_next_poll()


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
    coordinator.start_polling()

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
        data = hass.data[DOMAIN].pop(entry.entry_id, None)
        if data is not None:
            data["coordinator"].stop_polling()
    return unload_ok
