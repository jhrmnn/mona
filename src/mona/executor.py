# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Decentralized Execution Engine - coordinates task execution across hubs.

No central scheduler, DAG store, or state authority.
Execution is push-driven by completion signals.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

from .hashing import Hash, hash_text
from .messaging import Message, MessageBus, MessageType
from .recipe import Patch, PatchOperation, Recipe
from .registry import HubRegistry
from .taskhub import TaskHub, TaskRef

__all__ = ['DecentralizedExecutor']

log = logging.getLogger(__name__)


class DecentralizedExecutor:
    """Decentralized execution engine coordinating task hubs via messages.
    
    Features:
    - No central scheduler or state authority
    - Push-driven execution by completion signals
    - Recipe-based DAG that flows through the network
    """
    
    def __init__(self, registry: HubRegistry, message_bus: MessageBus):
        self.registry = registry
        self.message_bus = message_bus
        self._active_recipes: Dict[str, Recipe] = {}
    
    def compute_task_hash(self, function_identity: str, inputs: Any) -> Hash:
        """Compute task hash from function identity and canonicalized inputs.
        
        task_hash = H(function_identity, canonicalized_inputs)
        """
        # Convert inputs to JSON-serializable form
        def serialize_input(obj):
            if isinstance(obj, TaskRef):
                return obj.to_dict()
            return obj
        
        # Canonicalize inputs by JSON serialization with sorted keys
        if isinstance(inputs, (list, tuple)):
            serializable_inputs = [serialize_input(i) for i in inputs]
            canonical_inputs = json.dumps(serializable_inputs, sort_keys=True)
        else:
            canonical_inputs = json.dumps([serialize_input(inputs)], sort_keys=True)
        
        # Create hash from function identity and inputs
        hash_input = json.dumps([function_identity, canonical_inputs], sort_keys=True)
        return hash_text(hash_input)
    
    def execute_recipe(self, recipe: Recipe, output_task_hash: Hash) -> Any:
        """Execute a recipe to compute the output task.
        
        This drives the execution by:
        1. Processing tasks in the recipe
        2. Sending to appropriate hubs
        3. Handling completion signals
        4. Returning final result when output task is fully resolved
        """
        self._active_recipes[recipe.recipe_id] = recipe
        log.info(f"Starting execution of recipe {recipe.recipe_id}")
        
        # Process tasks iteratively to handle dynamic task creation
        max_iterations = 1000
        iteration = 0
        processed_tasks = set()
        
        while iteration < max_iterations:
            # Get unprocessed tasks
            current_tasks = set(recipe.tasks.keys()) - processed_tasks
            
            if not current_tasks:
                # No new tasks, check if output is ready
                hub = self.registry.get_hub(recipe.tasks[output_task_hash]["function_id"])
                if hub and hub.has_completed(output_task_hash):
                    result = hub.get_result(output_task_hash)
                    # Resolve any remaining TaskRefs in the result
                    resolved_result = self._resolve_inputs(result, recipe)
                    if self._is_fully_resolved(resolved_result):
                        log.info(f"Recipe {recipe.recipe_id} completed")
                        return resolved_result
                break
            
            # Process unprocessed tasks
            for task_hash in current_tasks:
                task_info = recipe.get_task_info(task_hash)
                if task_info and not recipe.is_complete(task_hash):
                    self._process_task(recipe, task_hash, task_info)
                processed_tasks.add(task_hash)
        
        # Deliver messages until completion
        max_iterations = 1000  # Safety limit
        iteration = 0
        while iteration < max_iterations:
            if self.message_bus.has_pending():
                delivered = self.message_bus.deliver_pending()
                log.debug(f"Delivered {delivered} messages")
            
            # Check if output task is complete
            hub = self.registry.get_hub(recipe.tasks[output_task_hash]["function_id"])
            if hub and hub.has_completed(output_task_hash):
                result = hub.get_result(output_task_hash)
                # Resolve any remaining TaskRefs in the result
                resolved_result = self._resolve_inputs(result, recipe)
                if self._is_fully_resolved(resolved_result):
                    log.info(f"Recipe {recipe.recipe_id} completed")
                    return resolved_result
            
            # If no pending messages and task not complete, we're stuck
            if not self.message_bus.has_pending():
                break
            
            iteration += 1
        
        raise RuntimeError(f"Recipe execution failed to complete after {iteration} iterations")
    
    def _process_task(self, recipe: Recipe, task_hash: Hash, task_info: Dict[str, Any]) -> None:
        """Process a single task by sending it to the appropriate hub."""
        function_id = task_info["function_id"]
        inputs = task_info["inputs"]
        
        hub = self.registry.get_hub(function_id)
        if not hub:
            raise RuntimeError(f"No hub registered for function: {function_id}")
        
        # Resolve any TaskRefs in inputs before execution
        resolved_inputs = self._resolve_inputs(inputs, recipe)
        
        # Execute task on hub
        result = hub.execute_task(task_hash, resolved_inputs)
        
        # Mark task as complete in recipe
        patch = Patch.create_mark_complete(task_hash)
        recipe.apply_patch(patch)
        
        # If result contains TaskRefs, handle them
        if self._contains_task_refs(result):
            log.debug(f"Task {task_hash} result contains TaskRefs, needs resolution")
            # In a full implementation, we'd set up waiting relationships
            # For now, we just note it
        
        # Send completion signal
        completion_msg = Message.create_task_completion(task_hash, result, function_id)
        self.message_bus.send(completion_msg)
    
    def _resolve_inputs(self, inputs: Any, recipe: Recipe) -> Any:
        """Resolve any TaskRefs in inputs to their actual values.
        
        This recursively processes TaskRefs and executes dependent tasks if needed.
        """
        if isinstance(inputs, TaskRef):
            # Get the task hash and resolve it
            task_hash = inputs.task_hash
            task_info = recipe.get_task_info(task_hash)
            if task_info:
                # Process the dependent task if not already complete
                if not recipe.is_complete(task_hash):
                    self._process_task(recipe, task_hash, task_info)
                # Get result from hub
                hub = self.registry.get_hub(task_info["function_id"])
                if hub:
                    result = hub.get_result(task_hash)
                    # Recursively resolve any TaskRefs in the result
                    return self._resolve_inputs(result, recipe)
            return inputs  # Return as-is if can't resolve
        elif isinstance(inputs, dict):
            if "$task" in inputs:
                # This is a TaskRef in dict form
                task_ref = TaskRef.from_dict(inputs)
                return self._resolve_inputs(task_ref, recipe)
            return {k: self._resolve_inputs(v, recipe) for k, v in inputs.items()}
        elif isinstance(inputs, (list, tuple)):
            resolved = [self._resolve_inputs(item, recipe) for item in inputs]
            return type(inputs)(resolved) if isinstance(inputs, tuple) else resolved
        return inputs
    
    def _contains_task_refs(self, obj: Any) -> bool:
        """Check if an object contains TaskRefs."""
        if isinstance(obj, TaskRef):
            return True
        if isinstance(obj, dict):
            if "$task" in obj:
                return True
            return any(self._contains_task_refs(v) for v in obj.values())
        if isinstance(obj, (list, tuple)):
            return any(self._contains_task_refs(item) for item in obj)
        return False
    
    def _is_fully_resolved(self, obj: Any) -> bool:
        """Check if an object is fully resolved (contains no TaskRefs)."""
        return not self._contains_task_refs(obj)
    
    def create_task_in_recipe(self, recipe: Recipe, function_identity: str, 
                             inputs: Any) -> Hash:
        """Create a new task in a recipe and return its hash."""
        task_hash = self.compute_task_hash(function_identity, inputs)
        
        # Create and apply patch to add task
        patch = Patch.create_add_task(task_hash, function_identity, inputs)
        if recipe.apply_patch(patch):
            log.debug(f"Added task {task_hash} to recipe {recipe.recipe_id}")
        
        return task_hash
    
    def add_dependency_edge(self, recipe: Recipe, from_task: Hash, to_task: Hash) -> None:
        """Add a dependency edge between tasks in a recipe."""
        patch = Patch.create_add_edge(from_task, to_task, "dependency")
        if recipe.apply_patch(patch):
            log.debug(f"Added dependency edge {from_task} -> {to_task}")
    
    def register_function_hub(self, function_identity: str, func: Callable[..., Any]) -> TaskHub:
        """Register a function with its own hub."""
        hub = TaskHub(function_identity, func)
        self.registry.register_hub(hub)
        
        # Register message handler for this hub
        def handle_message(msg: Message) -> None:
            if msg.msg_type == MessageType.TASK_COMPLETION:
                # Handle completion signal
                log.debug(f"Hub {function_identity} received completion: {msg}")
            elif msg.msg_type == MessageType.DEPENDENCY_REQUEST:
                # Handle dependency request
                task_hash = Hash(msg.data["task_hash"])
                result = hub.get_result(task_hash)
                if result is not None:
                    response = Message.create_dependency_response(
                        task_hash, result, function_identity, msg.from_hub or ""
                    )
                    self.message_bus.send(response)
        
        self.message_bus.register_handler(function_identity, handle_message)
        return hub
    
    def __repr__(self) -> str:
        return f"DecentralizedExecutor(hubs={len(self.registry.list_hubs())}, active_recipes={len(self._active_recipes)})"
