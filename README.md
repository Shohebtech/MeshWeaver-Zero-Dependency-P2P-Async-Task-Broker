# Module 4: Cloudpickle Task Serialization and Execution

1. Overview

Module 4 handles the serialization, deserialization, and execution of Python tasks in MeshWeaver. It uses the `cloudpickle` library to convert Python functions and their arguments into bytes so they can be transferred between compatible Python processes.

The module also provides a task executor that executes deserialized functions, returns their results or error information, and records task execution duration. A task state module tracks task status throughout its lifecycle.

2. Responsibilities

Module 4 is responsible for:

- Serializing Python functions and their arguments.
- Supporting positional and keyword arguments.
- Validating functions and task arguments before serialization.
- Deserializing serialized task payloads.
- Validating deserialized task payloads.
- Executing deserialized Python functions.
- Returning task execution results and error information.
- Distinguishing deserialization errors from execution errors.
- Tracking task status and results.
- Validating task identifiers.
- Measuring task execution duration.
- Providing unit tests for normal execution and failure scenarios.

3. Module Structure

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

### File Responsibilities

| File | Description |
|---|---|
| `serializer.py` | Serializes Python functions and arguments and deserializes task payloads. |
| `executor.py` | Executes serialized tasks and returns results, errors, and execution duration. |
| `task_state.py` | Tracks task identifiers, statuses, results, and errors. |
| `test_serializer.py` | Tests serialization, deserialization, and input validation. |
| `test_executor.py` | Tests task execution, execution timing, and error handling. |
| `test_task_state.py` | Tests task status management and task ID validation. |
| `test_end_to_end.py` | Tests the serialization-to-execution workflow. |

4. Installation

Install the required dependency using pip:

```bash
python -m pip install cloudpickle
```

Install pytest for running the tests:

```bash
python -m pip install pytest
```

5. Usage Example

The following example demonstrates how to serialize a Python function and execute it through the task executor.

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

Expected Output

The execution response will have the following structure:

```python
{
    'status': 'completed',
    'result': 30,
    'error': None,
    'execution_time': 0.00012
}
```

The `execution_time` value above is illustrative. The actual duration depends on the execution environment.

6. Task Serialization and Validation

The serializer converts a Python function and its arguments into a bytes payload using `cloudpickle`.

The serializer validates the following inputs:

- The task function must be callable.
- Positional arguments must be provided as a tuple or list.
- Keyword arguments must be provided as a dictionary.

List-based positional arguments are converted into tuples before serialization.

During deserialization, the module checks that:

- The serialized input is bytes.
- The restored payload is a dictionary.
- The task function is callable.
- The positional arguments are a tuple.
- The keyword arguments are a dictionary.

Invalid inputs produce appropriate exceptions.

7. Task Execution Flow

The task execution workflow consists of the following steps:

1. A Python function and its arguments are passed to the serializer.
2. The serializer validates the inputs.
3. The function and arguments are serialized into bytes.
4. The serialized payload is passed to the task executor.
5. The executor deserializes and validates the payload.
6. The restored function is executed with its arguments.
7. The executor measures the elapsed processing time.
8. The executor returns the task status, result, error information, and execution duration.

This workflow can be used as the basis for transferring tasks between compatible Python processes. Actual network transmission requires integration with the project's networking module.

8. Task State Management

The `task_state.py` module provides a `TaskState` class to track task information.

The supported task statuses are:

| Status | Description |
|---|---|
| `submitted` | The task has been created and submitted. |
| `running` | The task is being processed. |
| `completed` | The task has completed successfully. |
| `failed` | The task has encountered an error. |

Each task state stores:

- Task ID
- Current status
- Execution result
- Error information

Task IDs must be non-empty strings. Invalid identifiers are rejected during task state initialization.

When a task is marked as completed, its result is stored and any previous error is cleared. When a task is marked as failed, its error is stored and any previous result is cleared.

9. Error Handling and Execution Timing

The executor handles two main categories of errors.

### Deserialization Errors

These occur when the serialized payload cannot be restored or validated.

The response uses:

```python
{
    "status": "failed",
    "result": None,
    "error": "Deserialization error: ...",
    "execution_time": 0.00012
}
```

### Execution Errors

These occur when the restored function raises an exception during execution.

The response uses:

```python
{
    "status": "failed",
    "result": None,
    "error": "Execution error: ...",
    "execution_time": 0.00012
}
```

The execution duration is measured using Python's `time.perf_counter()`.

In the current implementation, the measured duration includes deserialization and function execution. Actual timing values depend on the environment and task.

10. Testing

Run all project tests from the repository root:

```bash
python -m pytest tests -v
```

Run individual test modules when debugging:

```bash
python -m pytest tests/test_serializer.py -v
python -m pytest tests/test_executor.py -v
python -m pytest tests/test_task_state.py -v
python -m pytest tests/test_end_to_end.py -v
```

The test suite covers:

- Function serialization and deserialization.
- Positional and keyword argument handling.
- Invalid serialization inputs.
- Task execution and return values.
- Execution timing.
- Deserialization and function execution errors.
- Task state transitions and result management.
- Task ID validation.
- End-to-end serialization and execution.

Test results should be verified in the current development environment before reporting the module as fully tested.

11. Security Considerations

**Only deserialize payloads from trusted sources.**

Deserializing untrusted data with `cloudpickle` can execute arbitrary code. Therefore, serialized task payloads must not be accepted from arbitrary peers.

Network authentication, authorization, and peer trust checks should be addressed as part of the broader MeshWeaver design.

12. Integration with Other Modules

Module 4 provides the serialization, deserialization, task execution, and task state functionality.

Integration with the networking and other MeshWeaver modules should follow the interfaces agreed upon by the project team.

The serializer and executor can process task payloads locally. Actual peer-to-peer task transmission and remote execution must be verified through integration with the networking module.

 13. Summary

Module 4 establishes the task serialization and execution functionality for MeshWeaver using Cloudpickle.

Its main features include:

- Function and argument serialization.
- Payload validation.
- Task deserialization and execution.
- Execution result and error reporting.
- Task state tracking.
- Task ID validation.
- Execution duration measurement.
- Unit and end-to-end testing.

These components provide a foundation for integrating task execution with the wider MeshWeaver peer-to-peer architecture.