"""
Test the decentralized task-hub execution framework.
"""
import pytest

from mona import DecentralizedExecutor, HubRegistry, MessageBus, Recipe, Rule, Session


@Rule
def add(x, y):
    """Simple addition function."""
    return x + y


@Rule
def multiply(x, y):
    """Simple multiplication function."""
    return x * y


@Rule
def compute(a, b, c):
    """Composite computation."""
    return add(multiply(a, b), c)


def test_basic_decentralized_execution():
    """Test basic task execution in decentralized mode."""
    with Session(decentralized=True) as sess:
        result = sess.eval(10)
        assert result == 10


def test_simple_task_execution():
    """Test simple task execution with decentralized backend."""
    with Session(decentralized=True) as sess:
        task = add(2, 3)
        result = sess.eval(task)
        assert result == 5


def test_multiple_tasks():
    """Test execution of multiple independent tasks."""
    with Session(decentralized=True) as sess:
        task1 = add(2, 3)
        task2 = multiply(4, 5)
        
        result1 = sess.eval(task1)
        result2 = sess.eval(task2)
        
        assert result1 == 5
        assert result2 == 20


def test_dependent_tasks():
    """Test execution of tasks with dependencies."""
    with Session(decentralized=True) as sess:
        task = compute(2, 3, 4)
        result = sess.eval(task)
        # Should compute: add(multiply(2, 3), 4) = add(6, 4) = 10
        assert result == 10


def test_hub_registry():
    """Test hub registry functionality."""
    from mona.taskhub import TaskHub
    
    registry = HubRegistry()
    
    hub1 = TaskHub("func1", lambda x: x * 2)
    hub2 = TaskHub("func2", lambda x: x + 1)
    
    registry.register_hub(hub1)
    registry.register_hub(hub2)
    
    assert registry.has_hub("func1")
    assert registry.has_hub("func2")
    assert not registry.has_hub("func3")
    
    assert registry.get_hub("func1") is hub1
    assert registry.get_hub("func2") is hub2


def test_recipe_creation():
    """Test recipe creation and patching."""
    from mona.recipe import Patch, PatchOperation
    from mona.hashing import Hash
    
    recipe = Recipe()
    
    # Add a task
    task_hash = Hash("abc123")
    patch1 = Patch.create_add_task(task_hash, "my_func", [1, 2])
    assert recipe.apply_patch(patch1)
    
    # Check task was added
    task_info = recipe.get_task_info(task_hash)
    assert task_info is not None
    assert task_info["function_id"] == "my_func"
    assert task_info["inputs"] == [1, 2]
    
    # Try to apply same patch again (should be deduplicated)
    assert not recipe.apply_patch(patch1)


def test_message_bus():
    """Test message bus functionality."""
    bus = MessageBus()
    
    messages_received = []
    
    def handler(msg):
        messages_received.append(msg)
    
    bus.register_handler("hub1", handler)
    
    from mona.messaging import Message, MessageType
    from mona.hashing import Hash
    
    # Send a message
    msg = Message.create_task_completion(Hash("task123"), 42, "hub2")
    msg.to_hub = "hub1"
    bus.send(msg)
    
    # Deliver messages
    delivered = bus.deliver_pending()
    assert delivered == 1
    assert len(messages_received) == 1
    assert messages_received[0].data["result"] == 42


def test_task_hub_execution():
    """Test task hub execution and caching."""
    from mona.taskhub import TaskHub
    from mona.hashing import Hash
    
    call_count = [0]
    
    def func(x):
        call_count[0] += 1
        return x * 2
    
    hub = TaskHub("test_func", func)
    
    task_hash = Hash("task123")
    
    # First execution
    result1 = hub.execute_task(task_hash, [5])
    assert result1 == 10
    assert call_count[0] == 1
    
    # Second execution (should use cache)
    result2 = hub.execute_task(task_hash, [5])
    assert result2 == 10
    assert call_count[0] == 1  # Should not have called func again


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
