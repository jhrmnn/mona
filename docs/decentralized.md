# Decentralized Task-Hub DAG Execution

This document describes the decentralized execution mode introduced in Mona, which provides a fully decentralized alternative to the traditional centralized execution model.

## Overview

In decentralized mode, Mona executes DAGs without any central scheduler, DAG store, or state authority. Instead:

- **Task Hubs**: Each function has its own hub responsible for executing tasks of that function
- **Recipes**: Portable, append-only DAG descriptions that flow through the network
- **TaskRefs**: References to tasks that enable dynamic graph expansion
- **Message-Based Communication**: Hubs exchange messages to coordinate execution

## Architecture

### Task Hubs

A TaskHub is a long-lived service responsible for exactly one function identity. Each hub:

```python
from mona import TaskHub

# Create a hub for a specific function
hub = TaskHub("my_function_id", my_function)

# Execute a task
task_hash = compute_task_hash("my_function_id", [arg1, arg2])
result = hub.execute_task(task_hash, [arg1, arg2])

# Results are cached automatically
cached_result = hub.get_result(task_hash)
```

Key responsibilities:
- Execute tasks of its function
- Cache completed task outputs
- Track unresolved task futures
- Exchange messages with other hubs

### Recipes

A Recipe is a portable, append-only DAG description:

```python
from mona import Recipe, Patch

# Create a new recipe
recipe = Recipe()

# Add a task via a patch
patch = Patch.create_add_task(task_hash, "function_id", inputs)
recipe.apply_patch(patch)

# Add dependency edges
patch = Patch.create_add_edge(from_task, to_task, "dependency")
recipe.apply_patch(patch)
```

Recipes contain:
- `recipe_id`: globally unique identifier
- Set of task nodes (identified by task_hash)
- Dependency edges
- Execution metadata
- Patch history for idempotent updates

### Task Identity

Tasks are identified by a hash of their function identity and canonicalized inputs:

```
task_hash = H(function_identity, canonicalized_inputs)
```

This ensures:
- Deterministic task identification
- Automatic deduplication
- Content-addressable execution

### TaskRefs and Dynamic Graph Expansion

Tasks can return TaskRefs (futures) instead of concrete values:

```python
@Rule
def compute(a, b, c):
    # These return TaskRefs, not values
    x = multiply(a, b)
    y = add(x, c)
    return y  # Returns a TaskRef
```

TaskRefs enable:
- Dynamic task creation during execution
- Futures as first-class data
- Automatic dependency tracking

## Using Decentralized Mode

### Basic Usage

Enable decentralized mode by passing `decentralized=True` to Session:

```python
from mona import Rule, Session

@Rule
def add(x, y):
    return x + y

@Rule
def compute(a, b, c):
    return add(add(a, b), c)

# Use decentralized execution
with Session(decentralized=True) as sess:
    result = sess.eval(compute(1, 2, 3))
    print(result)  # Output: 6
```

### Comparison with Centralized Mode

**Centralized (Traditional)**:
```python
with Session() as sess:
    result = sess.eval(my_task())
```

- Single central session manages all state
- Central scheduler coordinates execution
- All tasks tracked in one DAG

**Decentralized (New)**:
```python
with Session(decentralized=True) as sess:
    result = sess.eval(my_task())
```

- Each function has its own hub
- No central scheduler
- Recipe flows through network
- Message-based coordination

Both APIs produce the same results, ensuring backward compatibility.

## Implementation Details

### Hub Registry

Maps function identities to their hubs:

```python
from mona import HubRegistry

registry = HubRegistry()
registry.register_hub(hub)

# Lookup hub for a function
hub = registry.get_hub("my_function_id")
```

### Message Bus

Handles hub-to-hub communication:

```python
from mona import MessageBus, Message, MessageType

bus = MessageBus()

# Register a handler
bus.register_handler("hub_id", handler_function)

# Send messages
msg = Message.create_task_completion(task_hash, result, "from_hub")
bus.send(msg)

# Deliver pending messages
bus.deliver_pending()
```

### Execution Flow

1. **Task Creation**: User creates a task via `@Rule` decorated function
2. **Recipe Building**: Task and dependencies added to recipe via patches
3. **Hub Registration**: Function registered with its hub
4. **Task Execution**: Recipe processed, tasks sent to appropriate hubs
5. **Future Resolution**: TaskRefs resolved by executing dependent tasks
6. **Result Retrieval**: Final result returned to user

## Benefits

### Scalability
- No central bottleneck
- Each hub can run independently
- Natural distribution across machines

### Resilience
- Failure of one hub doesn't affect others
- Recipes can be replayed
- State is distributed

### Caching
- Results cached per-function in hubs
- Automatic deduplication via task hashing
- Content-addressable storage

## Current Limitations

This is a prototype implementation (v0). Intentionally deferred features:

- Durable message delivery
- Fault tolerance under permanent failures
- Distributed hub deployment (currently single-process)
- Global DAG visualization
- Load balancing per function
- Garbage collection of cached results
- Security and authentication
- Resource accounting across hubs

## Examples

See `examples/decentralized_fibonacci.py` for a complete working example comparing centralized and decentralized execution.

## API Reference

### Session

```python
Session(decentralized: bool = False)
```

Create a session with optional decentralized execution.

### TaskHub

```python
TaskHub(function_identity: str, func: Callable)
```

Hub for executing tasks of a specific function.

### Recipe

```python
Recipe(recipe_id: Optional[str] = None)
```

Portable DAG description with execution state.

### Patch

```python
Patch.create_add_task(task_hash, function_id, inputs)
Patch.create_add_edge(from_task, to_task, edge_type)
Patch.create_mark_complete(task_hash)
```

Operations to mutate recipes.

### TaskRef

```python
TaskRef(task_hash: Hash)
```

Reference to a task for future resolution.
