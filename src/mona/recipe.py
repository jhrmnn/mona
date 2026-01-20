# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Recipe - A portable, append-only DAG description with evolving execution state.

A recipe contains:
- recipe_id: globally unique identifier
- Set of task nodes (identified by task_hash)
- Dependency edges (including future-based edges)
- Execution metadata (optional)
- Patch history (implicit or explicit)
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from .hashing import Hash

__all__ = ['Recipe', 'Patch', 'PatchOperation']


class PatchOperation:
    """Represents different types of patch operations."""
    ADD_TASK = "add_task"
    ADD_EDGE = "add_edge"
    MARK_COMPLETE = "mark_complete"


class Patch:
    """Represents a mutation to a recipe.
    
    patch := (patch_id, operation)
    patch_id MUST be deterministic and unique
    """
    
    def __init__(self, patch_id: str, operation: str, data: Dict[str, Any]):
        self.patch_id = patch_id
        self.operation = operation
        self.data = data
    
    @classmethod
    def create_add_task(cls, task_hash: Hash, function_id: str, inputs: Any) -> Patch:
        """Create a patch to add a task node."""
        data = {
            "task_hash": task_hash,
            "function_id": function_id,
            "inputs": inputs
        }
        # Create deterministic patch_id from operation and data
        patch_content = json.dumps([PatchOperation.ADD_TASK, data], sort_keys=True)
        patch_id = hashlib.sha1(patch_content.encode()).hexdigest()
        return cls(patch_id, PatchOperation.ADD_TASK, data)
    
    @classmethod
    def create_add_edge(cls, from_task: Hash, to_task: Hash, edge_type: str = "dependency") -> Patch:
        """Create a patch to add a dependency edge."""
        data = {
            "from": from_task,
            "to": to_task,
            "type": edge_type
        }
        patch_content = json.dumps([PatchOperation.ADD_EDGE, data], sort_keys=True)
        patch_id = hashlib.sha1(patch_content.encode()).hexdigest()
        return cls(patch_id, PatchOperation.ADD_EDGE, data)
    
    @classmethod
    def create_mark_complete(cls, task_hash: Hash) -> Patch:
        """Create a patch to mark a task as complete."""
        data = {"task_hash": task_hash}
        patch_content = json.dumps([PatchOperation.MARK_COMPLETE, data], sort_keys=True)
        patch_id = hashlib.sha1(patch_content.encode()).hexdigest()
        return cls(patch_id, PatchOperation.MARK_COMPLETE, data)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "operation": self.operation,
            "data": self.data
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Patch:
        return cls(data["patch_id"], data["operation"], data["data"])
    
    def __repr__(self) -> str:
        return f"Patch({self.patch_id[:8]}, {self.operation})"


class Recipe:
    """A portable, append-only DAG description with evolving execution state.
    
    Recipes are logically immutable; updates are applied as patches.
    """
    
    def __init__(self, recipe_id: Optional[str] = None):
        self.recipe_id = recipe_id or str(uuid.uuid4())
        self.tasks: Dict[Hash, Dict[str, Any]] = {}  # task_hash -> {function_id, inputs}
        self.edges: List[Tuple[Hash, Hash, str]] = []  # (from, to, type)
        self.completed_tasks: Set[Hash] = set()
        self.patches: List[Patch] = []
    
    def apply_patch(self, patch: Patch) -> bool:
        """Apply a patch to the recipe. Returns True if applied, False if duplicate."""
        # Check for duplicate
        if any(p.patch_id == patch.patch_id for p in self.patches):
            return False
        
        if patch.operation == PatchOperation.ADD_TASK:
            task_hash = Hash(patch.data["task_hash"])
            self.tasks[task_hash] = {
                "function_id": patch.data["function_id"],
                "inputs": patch.data["inputs"]
            }
        elif patch.operation == PatchOperation.ADD_EDGE:
            from_task = Hash(patch.data["from"])
            to_task = Hash(patch.data["to"])
            edge_type = patch.data.get("type", "dependency")
            self.edges.append((from_task, to_task, edge_type))
        elif patch.operation == PatchOperation.MARK_COMPLETE:
            task_hash = Hash(patch.data["task_hash"])
            self.completed_tasks.add(task_hash)
        
        self.patches.append(patch)
        return True
    
    def get_task_info(self, task_hash: Hash) -> Optional[Dict[str, Any]]:
        """Get information about a task."""
        return self.tasks.get(task_hash)
    
    def get_dependencies(self, task_hash: Hash) -> List[Hash]:
        """Get tasks that this task depends on."""
        return [edge[0] for edge in self.edges if edge[1] == task_hash and edge[2] == "dependency"]
    
    def get_dependents(self, task_hash: Hash) -> List[Hash]:
        """Get tasks that depend on this task."""
        return [edge[1] for edge in self.edges if edge[0] == task_hash and edge[2] == "dependency"]
    
    def is_complete(self, task_hash: Hash) -> bool:
        """Check if a task is marked as complete."""
        return task_hash in self.completed_tasks
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize recipe to dictionary."""
        return {
            "recipe_id": self.recipe_id,
            "tasks": {k: v for k, v in self.tasks.items()},
            "edges": [(str(f), str(t), typ) for f, t, typ in self.edges],
            "completed_tasks": list(self.completed_tasks),
            "patches": [p.to_dict() for p in self.patches]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Recipe:
        """Deserialize recipe from dictionary."""
        recipe = cls(data["recipe_id"])
        recipe.tasks = {Hash(k): v for k, v in data["tasks"].items()}
        recipe.edges = [(Hash(f), Hash(t), typ) for f, t, typ in data["edges"]]
        recipe.completed_tasks = set(Hash(h) for h in data["completed_tasks"])
        recipe.patches = [Patch.from_dict(p) for p in data["patches"]]
        return recipe
    
    def __repr__(self) -> str:
        return f"Recipe({self.recipe_id[:8]}, tasks={len(self.tasks)}, patches={len(self.patches)})"
