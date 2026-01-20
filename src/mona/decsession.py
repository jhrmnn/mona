# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Decentralized Session - bridges the original Session API with decentralized execution.

This layer maintains API compatibility while using the new decentralized backend.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from .executor import DecentralizedExecutor
from .hashing import Hash
from .messaging import MessageBus
from .recipe import Recipe
from .registry import HubRegistry
from .utils import fullname_of

__all__ = ['DecentralizedSession']

log = logging.getLogger(__name__)


class DecentralizedSession:
    """Session that uses decentralized task-hub execution.
    
    This maintains the high-level Session API but delegates to
    the decentralized executor backend.
    """
    
    def __init__(self):
        self.registry = HubRegistry()
        self.message_bus = MessageBus()
        self.executor = DecentralizedExecutor(self.registry, self.message_bus)
        self._current_recipe: Optional[Recipe] = None
        self._function_hubs: dict[str, Any] = {}
    
    def __enter__(self) -> DecentralizedSession:
        """Enter context manager."""
        log.debug("Entering DecentralizedSession")
        self._current_recipe = Recipe()
        return self
    
    def __exit__(self, exc_type: Any, *args: Any) -> None:
        """Exit context manager."""
        log.debug("Exiting DecentralizedSession")
        self._current_recipe = None
    
    def create_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Create a task in the decentralized system.
        
        This registers the function with a hub if not already registered,
        and adds the task to the current recipe.
        """
        if not self._current_recipe:
            raise RuntimeError("No active recipe. Use session context manager.")
        
        # Get or create hub for this function
        function_identity = fullname_of(func)
        if function_identity not in self._function_hubs:
            hub = self.executor.register_function_hub(function_identity, func)
            self._function_hubs[function_identity] = hub
        
        # Create task in recipe
        task_hash = self.executor.create_task_in_recipe(
            self._current_recipe, function_identity, args
        )
        
        # Return a handle that can be evaluated later
        return DecentralizedTaskHandle(task_hash, self)
    
    def eval(self, obj: Any, **kwargs: Any) -> Any:
        """Evaluate an object by executing all tasks it references.
        
        This is the main entry point for executing DAGs in the decentralized system.
        """
        if not self._current_recipe:
            raise RuntimeError("No active recipe. Use session context manager.")
        
        # If obj is a task handle, execute the recipe to get the result
        if isinstance(obj, DecentralizedTaskHandle):
            return self.executor.execute_recipe(self._current_recipe, obj.task_hash)
        
        # Otherwise, return as-is (pass-through for non-task objects)
        return obj
    
    def run_task(self, task: Any) -> Any:
        """Run a single task.
        
        In the decentralized model, tasks are executed by hubs as part
        of recipe execution.
        """
        return self.eval(task)


class DecentralizedTaskHandle:
    """Handle to a task in the decentralized system.
    
    This acts as a placeholder that will be resolved when eval() is called.
    """
    
    def __init__(self, task_hash: Hash, session: DecentralizedSession):
        self.task_hash = task_hash
        self.session = session
    
    def __repr__(self) -> str:
        return f"DecentralizedTaskHandle({self.task_hash[:8]})"
