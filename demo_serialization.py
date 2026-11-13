
from meshweaver.serializer import serialize_task, deserialize_task
from meshweaver.executor import execute_serialized_task


# Step 1: Define a function
def calculate_total(price, quantity, tax=0):
    return (price * quantity) + tax


print("\n======================================")
print("     MESHWEAVER SERIALIZATION DEMO")
print("======================================")

# Step 2: Display the original function
print("\n[STEP 1] ORIGINAL TASK")
print("Function:", calculate_total.__name__)
print("Arguments:", (100, 2))
print("Keyword Arguments:", {"tax": 10})

# Step 3: Serialize the function and arguments
print("\n[STEP 2] SERIALIZATION")

serialized_data = serialize_task(
    calculate_total,
    args=(100, 2),
    kwargs={"tax": 10}
)

print("Serialization successful!")
print("Data type:", type(serialized_data))
print("Serialized data size:", len(serialized_data), "bytes")
print("Serialized bytes preview:", serialized_data[:50])

# Step 4: Deserialize the bytes
print("\n[STEP 3] DESERIALIZATION")

restored_task = deserialize_task(serialized_data)

print("Deserialization successful!")
print("Restored function:", restored_task["function"].__name__)
print("Restored arguments:", restored_task["args"])
print("Restored keyword arguments:", restored_task["kwargs"])

# Step 5: Execute the serialized task
print("\n[STEP 4] TASK EXECUTION")

response = execute_serialized_task(serialized_data)

print("Task Status:", response["status"])
print("Execution Result:", response["result"])
print("Error:", response["error"])
print("Execution Time:", response["execution_time"])

print("\n======================================")
print("       DEMO COMPLETED")
print("======================================")
