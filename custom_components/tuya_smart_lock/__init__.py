"""Tuya Smart Lock integration."""

import logging
from datetime import datetime

import homeassistant.util.dt as dt_util
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import CONF_ACCESS_ID, CONF_ACCESS_SECRET, CONF_API_REGION, CONF_DEVICE_ID, DOMAIN
from .tuya_api import TuyaCloudApi

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.LOCK, Platform.SENSOR, Platform.BINARY_SENSOR]

# The lock entity primarily stays on-demand (fetches on load and after
# each lock/unlock's verification window) rather than polling on its own.
# But it also listens to this coordinator's periodic reads and adopts
# whichever of the two - its own last on-demand fetch, or the
# coordinator's last periodic poll - is more recent (see
# last_success_time below and lock.py's _handle_coordinator_update).
# That matters because only the coordinator's poll would ever notice a
# lock/unlock done outside HA (physical keypad, the Tuya app, etc.) - the
# lock entity's on-demand-only fetches never would on their own.
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
        self.last_success_time: datetime | None = None

    async def _async_update_data(self) -> dict:
        dps = await self._api.async_get_status(self._device_id)
        if dps is None:
            raise UpdateFailed("Failed to fetch device status")
        self.last_success_time = dt_util.utcnow()
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
        """Refresh once, then always reschedule - even if the refresh failed.

        Confirmed by testing: an earlier version called _schedule_next_poll()
        only after an unguarded await self.async_refresh(), so any exception
        there (even a rare, transient one) would propagate out of this
        callback and permanently end the poll loop with no visible error.
        Wrapping both steps independently means one bad cycle can no longer
        kill polling for good.
        """
        self._cancel_next_poll = None
        try:
            await self.async_refresh()
        except Exception:
            _LOGGER.exception("Unexpected error refreshing status")
        try:
            self._schedule_next_poll()
        except Exception:
            _LOGGER.exception("Unexpected error scheduling next poll")


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
