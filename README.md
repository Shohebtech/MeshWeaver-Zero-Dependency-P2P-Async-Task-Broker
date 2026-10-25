Module 4: Cloudpickle Task Serialization and Execution

Overview

Module 4 handles the serialization and deserialization of Python tasks in MeshWeaver. It uses the `cloudpickle` library to convert Python functions and their arguments into bytes so they can be transferred between compatible Python processes.

The module also provides a task executor that runs a deserialized function and returns its result or error information.

Responsibilities

- Serialize Python functions and their arguments.
- Support positional and keyword arguments.
- Deserialize serialized task payloads.
- Execute deserialized functions.
- Return task execution results.
- Handle deserialization and execution errors.
- Track task status using the task state module.

Module Structure

```text
meshweaver/
├── __init__.py
├── serializer.py
├── executor.py
└── task_state.py

tests/
├── test_serializer.py
├── test_executor.py
├── test_task_state.py
└── test_end_to_end.py
```

Installation

Install the required dependency:

```bash
python -m pip install cloudpickle
```

Usage Example

```python
from meshweaver.serializer import serialize_task
from meshweaver.executor import execute_serialized_task


def add_numbers(a, b):
    return a + b


# Serialize the function and its arguments.
payload = serialize_task(add_numbers, (10, 20))

# Execute the serialized task.
response = execute_serialized_task(payload)

print(response)
```

Expected output:

```python
{
    'status': 'completed',
    'result': 30,
    'error': None
}
```

Task Execution Flow

1. A Python function and its arguments are passed to the serializer.
2. The serializer converts the task into a bytes payload.
3. The payload is passed to the task executor.
4. The executor deserializes the payload.
5. The function is executed with the supplied arguments.
6. The executor returns the execution status, result, or error.

Error Handling

The executor returns a response with a `completed` status when execution succeeds.

If deserialization or function execution fails, it returns a `failed` status with an error message.

Testing

Run all Module 4 tests from the project root:

```bash
python -m pytest tests -v
```

The tests cover serialization, deserialization, task execution, task state tracking, keyword arguments, return values, and error handling.

Security Consideration

**Only deserialize payloads from trusted peers.** Deserializing untrusted data with `cloudpickle` can execute arbitrary code. Network authentication and trust checks must be handled as part of the broader MeshWeaver design.

Integration Note

The serializer and executor provide the task serialization and execution functionality. Network transmission and integration with the other MeshWeaver modules should follow the interfaces agreed upon by the project team.