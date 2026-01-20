# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Task Hub - A decentralized service responsible for exactly one function identity.

Each hub:
- Executes tasks of its function
- Caches completed task outputs
- Tracks unresolved task futures
- Exchanges messages with other hubs
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from .hashing import Hash

__all__ = ['TaskHub', 'TaskRef']

log = logging.getLogger(__name__)


class TaskRef:
    """A structured reference to a task (future).
    
    TaskRef := { "$task": task_hash }
    """
    
    def __init__(self, task_hash: Hash):
        self.task_hash = task_hash
    
    def to_dict(self) -> Dict[str, str]:
        return {"$task": self.task_hash}
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> TaskRef:
        return cls(Hash(data["$task"]))
    
    def __repr__(self) -> str:
        return f"TaskRef({self.task_hash})"


class TaskHub:
    """A long-lived service responsible for exactly one function identity.
    
    Maintains:
    - completed[task_hash] -> value: cached results
    - waiting[task_hash] -> list of (parent_task_hash, json_pointer): unresolved futures
    """
    
    def __init__(self, function_identity: str, func: Callable[..., Any]):
        self.function_identity = function_identity
        self.func = func
        self.completed: Dict[Hash, Any] = {}
        self.waiting: Dict[Hash, List[Tuple[Hash, str]]] = {}
        self._seen_patch_ids: Dict[str, set] = {}  # recipe_id -> set of patch_ids
    
    def has_completed(self, task_hash: Hash) -> bool:
        """Check if a task has been completed and cached."""
        return task_hash in self.completed
    
    def get_result(self, task_hash: Hash) -> Optional[Any]:
        """Get the cached result for a task."""
        return self.completed.get(task_hash)
    
    def execute_task(self, task_hash: Hash, inputs: Any) -> Any:
        """Execute a task and cache the result.
        
        Returns the result which may contain TaskRefs.
        """
        if task_hash in self.completed:
            log.debug(f"Hub {self.function_identity}: Task {task_hash} already completed, reusing")
            return self.completed[task_hash]
        
        log.info(f"Hub {self.function_identity}: Executing task {task_hash}")
        result = self.func(*inputs) if isinstance(inputs, (list, tuple)) else self.func(inputs)
        
        # Store result (may contain TaskRefs)
        self.completed[task_hash] = result
        log.debug(f"Hub {self.function_identity}: Task {task_hash} completed")
        
        return result
    
    def add_waiting(self, task_hash: Hash, parent_hash: Hash, json_pointer: str) -> None:
        """Register a parent task waiting for this task to complete."""
        if task_hash not in self.waiting:
            self.waiting[task_hash] = []
        self.waiting[task_hash].append((parent_hash, json_pointer))
    
    def get_waiting(self, task_hash: Hash) -> List[Tuple[Hash, str]]:
        """Get list of parent tasks waiting for this task."""
        return self.waiting.get(task_hash, [])
    
    def mark_patch_seen(self, recipe_id: str, patch_id: str) -> bool:
        """Mark a patch as seen for deduplication. Returns True if new, False if duplicate."""
        if recipe_id not in self._seen_patch_ids:
            self._seen_patch_ids[recipe_id] = set()
        
        if patch_id in self._seen_patch_ids[recipe_id]:
            return False
        
        self._seen_patch_ids[recipe_id].add(patch_id)
        return True
    
    def __repr__(self) -> str:
        return f"TaskHub({self.function_identity}, completed={len(self.completed)})"
