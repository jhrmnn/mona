# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Hub Registry - maintains mapping of function identities to hub addresses.

Each hub knows the mapping:
  function_identity → hub_address

This mapping is assumed to be globally consistent at startup.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

from .taskhub import TaskHub

__all__ = ['HubRegistry']

log = logging.getLogger(__name__)


class HubRegistry:
    """Registry maintaining function_identity -> hub mapping.
    
    In this prototype, hubs are managed within the same process.
    In a distributed system, this would map to network addresses.
    """
    
    def __init__(self):
        self._hubs: Dict[str, TaskHub] = {}
    
    def register_hub(self, hub: TaskHub) -> None:
        """Register a task hub for a function identity."""
        if hub.function_identity in self._hubs:
            log.warning(f"Hub for {hub.function_identity} already registered, replacing")
        self._hubs[hub.function_identity] = hub
        log.info(f"Registered hub for function: {hub.function_identity}")
    
    def get_hub(self, function_identity: str) -> Optional[TaskHub]:
        """Get the hub responsible for a function identity."""
        return self._hubs.get(function_identity)
    
    def get_hub_id(self, function_identity: str) -> Optional[str]:
        """Get the hub ID (same as function_identity in this prototype)."""
        if function_identity in self._hubs:
            return function_identity
        return None
    
    def has_hub(self, function_identity: str) -> bool:
        """Check if a hub is registered for a function identity."""
        return function_identity in self._hubs
    
    def list_hubs(self) -> list[str]:
        """List all registered hub function identities."""
        return list(self._hubs.keys())
    
    def __repr__(self) -> str:
        return f"HubRegistry(hubs={len(self._hubs)})"
