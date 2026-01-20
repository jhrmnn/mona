#!/usr/bin/env python
"""
Example demonstrating the decentralized task-hub execution framework.

This example shows how to use the decentralized mode where:
- Each function has its own TaskHub
- Execution is fully decentralized (no central scheduler)
- DAG structure is carried by Recipes that flow through the network
- Tasks are identified by hash of (function_identity, inputs)
"""

from mona import Rule, Session


@Rule
def fibonacci(n):
    """Compute Fibonacci number recursively."""
    if n <= 2:
        return 1
    return add(fibonacci(n - 1), fibonacci(n - 2))


@Rule
def add(x, y):
    """Add two numbers."""
    return x + y


def main():
    print("=== Centralized Execution (Original) ===")
    with Session() as sess:
        result = sess.eval(fibonacci(6))
        print(f"fibonacci(6) = {result}")
    
    print("\n=== Decentralized Execution (New) ===")
    with Session(decentralized=True) as sess:
        result = sess.eval(fibonacci(6))
        print(f"fibonacci(6) = {result}")
    
    print("\n✓ Both modes produce the same result!")
    print("\nIn decentralized mode:")
    print("- Each function (fibonacci, add) has its own TaskHub")
    print("- No central scheduler coordinates execution")
    print("- Tasks are identified by hash(function_identity, inputs)")
    print("- Results are cached in hubs for reuse")
    print("- Recipe carries the DAG structure and execution state")


if __name__ == "__main__":
    main()
