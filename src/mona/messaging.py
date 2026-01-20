# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Messaging infrastructure for decentralized hub communication.

Messages:
- Recipe + patch propagation
- Task completion signals
- Dependency resolution requests (optional pull fallback)
"""
from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Dict, Optional

from .hashing import Hash
from .recipe import Patch, Recipe

__all__ = ['Message', 'MessageType', 'MessageBus']

log = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages exchanged between hubs."""
    RECIPE_PATCH = "recipe_patch"
    TASK_COMPLETION = "task_completion"
    DEPENDENCY_REQUEST = "dependency_request"
    DEPENDENCY_RESPONSE = "dependency_response"


class Message:
    """A message exchanged between task hubs."""
    
    def __init__(self, msg_type: MessageType, data: Dict[str, Any], 
                 from_hub: Optional[str] = None, to_hub: Optional[str] = None):
        self.msg_type = msg_type
        self.data = data
        self.from_hub = from_hub
        self.to_hub = to_hub
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_type": self.msg_type.value,
            "data": self.data,
            "from_hub": self.from_hub,
            "to_hub": self.to_hub
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Message:
        return cls(
            MessageType(data["msg_type"]),
            data["data"],
            data.get("from_hub"),
            data.get("to_hub")
        )
    
    @classmethod
    def create_recipe_patch(cls, recipe: Recipe, patch: Patch, 
                           from_hub: str, to_hub: str) -> Message:
        """Create a message for recipe + patch propagation."""
        return cls(
            MessageType.RECIPE_PATCH,
            {
                "recipe": recipe.to_dict(),
                "patch": patch.to_dict()
            },
            from_hub,
            to_hub
        )
    
    @classmethod
    def create_task_completion(cls, task_hash: Hash, result: Any, 
                              from_hub: str) -> Message:
        """Create a message for task completion signal."""
        return cls(
            MessageType.TASK_COMPLETION,
            {
                "task_hash": task_hash,
                "result": result
            },
            from_hub
        )
    
    @classmethod
    def create_dependency_request(cls, task_hash: Hash, 
                                  from_hub: str, to_hub: str) -> Message:
        """Create a message to request task result (pull fallback)."""
        return cls(
            MessageType.DEPENDENCY_REQUEST,
            {"task_hash": task_hash},
            from_hub,
            to_hub
        )
    
    @classmethod
    def create_dependency_response(cls, task_hash: Hash, result: Any,
                                   from_hub: str, to_hub: str) -> Message:
        """Create a message with requested task result."""
        return cls(
            MessageType.DEPENDENCY_RESPONSE,
            {
                "task_hash": task_hash,
                "result": result
            },
            from_hub,
            to_hub
        )
    
    def __repr__(self) -> str:
        return f"Message({self.msg_type.value}, from={self.from_hub}, to={self.to_hub})"


class MessageBus:
    """Message bus for hub-to-hub communication.
    
    In this simplified prototype, messages are delivered synchronously
    within the same process. In a real distributed system, this would
    use network protocols, message queues, etc.
    """
    
    def __init__(self):
        self._message_queue: list[Message] = []
        self._handlers: Dict[str, Any] = {}  # hub_id -> handler callback
    
    def register_handler(self, hub_id: str, handler: Any) -> None:
        """Register a message handler for a hub."""
        self._handlers[hub_id] = handler
        log.debug(f"Registered message handler for hub: {hub_id}")
    
    def send(self, message: Message) -> None:
        """Send a message to its target hub(s).
        
        Messages may be duplicated or reordered.
        Deduplication is mandatory at the hub level.
        """
        if message.to_hub:
            # Unicast to specific hub
            self._message_queue.append(message)
            log.debug(f"Queued message: {message}")
        else:
            # Broadcast (for completion signals)
            for hub_id in self._handlers:
                if hub_id != message.from_hub:
                    msg = Message(message.msg_type, message.data, message.from_hub, hub_id)
                    self._message_queue.append(msg)
    
    def deliver_pending(self) -> int:
        """Deliver all pending messages to their handlers.
        
        Returns the number of messages delivered.
        """
        count = 0
        while self._message_queue:
            message = self._message_queue.pop(0)
            
            if message.to_hub and message.to_hub in self._handlers:
                handler = self._handlers[message.to_hub]
                try:
                    handler(message)
                    count += 1
                except Exception as e:
                    log.error(f"Error handling message {message}: {e}")
            elif not message.to_hub:
                # Broadcast - shouldn't happen in delivered messages
                log.warning(f"Broadcast message in delivery queue: {message}")
        
        return count
    
    def has_pending(self) -> bool:
        """Check if there are pending messages."""
        return len(self._message_queue) > 0
    
    def __repr__(self) -> str:
        return f"MessageBus(pending={len(self._message_queue)}, handlers={len(self._handlers)})"
