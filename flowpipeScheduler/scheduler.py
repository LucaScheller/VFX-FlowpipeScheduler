import json
import logging
import os
import sys
from tempfile import gettempdir

import redis
from flowpipe import Graph, INode, Node
from deadlineAPI.Deadline import Jobs as DLJobs

# -----------------------------------------------------------------------------
#
# Configuration
#
# -----------------------------------------------------------------------------


class EnvironmentVariables:
    identifier = "FP_DL_IDENTIFIER"
    database_type = "FP_DL_DATABASE_TYPE"
    batch_range = "FP_DL_BATCH_RANGE"


# -----------------------------------------------------------------------------
#
# Database
#
# -----------------------------------------------------------------------------


class RedisDatabase:
    """This stores the JSON-serialized nodes as string values in a Redis database."""

    REDIS_HOST = "localhost"
    REDIS_PORT = "6379"

    @classmethod
    def set(cls, node: Node):
        """Store the node under it's identifier.
        Args:
            node (Node): The node to serialize into the database.
        Returns:
            str: The database identifier.
        Raises:
            TypeError: Node contains non-serializable data.
        """
        identifier = node.identifier
        raw_data = json.dumps(node.serialize())

        server_connection = redis.Redis(
            host=cls.REDIS_HOST, port=cls.REDIS_PORT, decode_responses=True
        )
        server_connection.set(identifier, raw_data)
        return identifier

    @classmethod
    def get(cls, identifier: str):
        """Retrieve the node behind the given identifier.
        Args:
            identifier: The node to deserialize from the database.
        Returns:
            node: The node object.
        Raises:
            ValueError: Identifier can't be found.
            json.decoder.JSONDecodeError: Identifiers values can't be json deserialized.
        """
        server_connection = redis.Redis(
            host=cls.REDIS_HOST, port=cls.REDIS_PORT, decode_responses=True
        )

        raw_data = server_connection.get(identifier)
        if not raw_data:
            raise ValueError("Invalid identifier specified!")
        data = json.load(raw_data)
        return INode.deserialize(data)


"""
def get_redis_database():
    instance = getattr(get_redis_database, "instance", None)
    if not instance:
        instance = get_redis_database.instance = RedisDatabase()
    return instance
"""


class JsonDatabase:
    """This stores the JSON-serialized nodes as .json on-disk files."""

    PATH = os.path.join(gettempdir(), "json-database", "{identifier}.json")

    @staticmethod
    def set(node):
        """Store the node under it's identifier."""
        json_file_path = JsonDatabase.PATH.format(identifier=node.identifier)
        json_dir_path = os.path.dirname(json_file_path)
        if not os.path.exists(json_dir_path):
            os.makedirs(json_dir_path)
        with open(json_file_path, "w") as json_file:
            json.dump(node.serialize(), json_file, indent=2)
        return json_file_path

    @staticmethod
    def get(identifier):
        """Retrieve the node behind the given identifier."""
        json_file_path = JsonDatabase.PATH.format(identifier=identifier)
        with open(json_file_path, "r") as json_file:
            data = json.load(json_file)
        return INode.deserialize(data)


class DatabaseType:
    RedisDatabase = "REDIS"
    JsonDatabase = "JSON"


def get_database(database_type):
    if database_type == DatabaseType.RedisDatabase:
        return RedisDatabase
    elif database_type == DatabaseType.JsonDatabase:
        return JsonDatabase
    else:
        raise Exception("Invalid database specified | {}".format(database_type))


# -----------------------------------------------------------------------------
#
# Conversion
#
# -----------------------------------------------------------------------------


# Command templates for different interpreters
COMMANDS = {
    "python": (
        "python -c '"
        "from flowpipeDeadline import scheduler;"
        'scheduler.evaluate_on_farm("{serialized_json}", batch_range={batch_items}, database_type={database})\''
    ),
    "houdini": (
        "hython -c '"
        "from flowpipeDeadline import scheduler;"
        'scheduler.evaluate_on_farm("{serialized_json}", batch_range={batch_items}, database_type={database})\''
    ),
}

COMMANDS_INTERPRETER = {
    "python": "python",
    "python_39": "python3.9",
    "python_311": "python3.11",
    "python_312": "python3.12",
    "houdini": "hython",
}
COMMANDS_IMPLICIT_ENV = (
    "from flowpipeDeadline import scheduler; scheduler.evaluate_on_farm_through_env()"
)


# -----------------------------------------------------------------------------
#
# Conversion | Deadline
#
# -----------------------------------------------------------------------------


def dl_convert_graph_to_job(graph, database_type=DatabaseType.RedisDatabase):
    """Convert the graph to a dict representing a typical render farm job."""

    dl_job = DLJobs.Job()
    dl_job.JobBatchName = graph.name

    job = {"name": graph.name, "tasks": []}

    # Turn every node into a farm task
    tasks = {}
    for node in graph.nodes:
        serialized_json = database.set(node)

        tasks[node.name] = []

        # IMPLICIT BATCHING:
        # Create individual tasks for each batch if the batch size is defined
        # Feed the calculated frame range to each batch
        node_batch_size = node.metadata.get("batch_size")
        node_input_batch_items = node.inputs.get("batch_items")
        if node_batch_size and node_input_batch_items:
            batch_size = node.metadata["batch_size"]
            batch_items = node_input_batch_items.value
            batch_index = 0
            while batch_index < len(batch_items) - 1:
                batch_end = batch_index + batch_size
                if batch_end > len(batch_items) - 1:
                    batch_end = len(batch_items)
                batch_item_range = batch_items[batch_index:batch_end]

                task = {"name": "{0}-{1}".format(node.name, batch_index / batch_size)}
                command = COMMANDS.get(node.metadata.get("interpreter", "python"), None)

                task["command"] = command.format(
                    serialized_json=serialized_json,
                    batch_items=batch_item_range,
                    database=database,
                )
                job["tasks"].append(task)

                tasks[node.name].append(task)

                batch_index += batch_size
        else:
            task = {"name": node.name}
            command = COMMANDS.get(node.metadata.get("interpreter", "python"), None)
            task["command"] = command.format(
                serialized_json=serialized_json, batch_items=None, database=database
            )
            job["tasks"].append(task)

            tasks[node.name].append(task)

    # The dependencies between the tasks based on the connections of the Nodes
    for node_name in tasks:
        for task in tasks[node_name]:
            node = graph[node_name]
            task["dependencies"] = []
            for upstream in [n.name for n in node.upstream_nodes]:
                task["dependencies"] += [t["name"] for t in tasks[upstream]]

    return job


# -----------------------------------------------------------------------------
#
# Evaluation
#
# -----------------------------------------------------------------------------


def evaluate_on_farm_through_env():
    """Evaluate on farm by extracting the evaluation data through environment variables."""
    identifier = os.environ[EnvironmentVariables.identifier]
    batch_range = os.environ[EnvironmentVariables.batch_range]
    database_type = os.environ[EnvironmentVariables.database_type]
    evaluate_on_farm(identifier, batch_range, database_type)


def evaluate_on_farm(
    serialized_json, batch_range=None, database_type=DatabaseType.RedisDatabase
):
    """Evaluate the node behind the given json file.

    1. Deserialize the node
    2. Collect any input values from any upstream dependencies
        For implicit batching, the given frames are assigned to the node,
        overriding whatever might be stored in the json file, becuase all
        batches share the same json file.
    3. Evaluate the node
    4. Serialize the node back into its original file
        For implicit farm conversion, the serialization only happens once,
        for the 'last' batch, knowing that the last batch in numbers might
        not be the 'last' batch actually executed.
    """
    # Debug logs might be useful on the farm
    logging.baseConfig.setLevel(logging.DEBUG)

    database = get_database(database_type)

    # Deserialize the node from the serialized json
    with open(serialized_json, "r") as f:
        data = json.load(f)
    node = INode.deserialize(data)

    # Retrieve the upstream output data
    for name, input_plug in data["inputs"].items():
        for identifier, output_plug in input_plug["connections"].items():
            upstream_node = database.get(identifier)
            node.inputs[name].value = upstream_node.outputs[output_plug].value

    # Specifically assign the batch frames here if applicable
    if batch_range is not None:
        all_batch_items = node.inputs["batch_items"]
        node.inputs["batch_items"] = all_batch_items[
            batch_range[0] : batch_range[1] + 1 : batch_range[2]
        ]

    # Actually evalute the node
    node.evaluate()

    # Store the result back into the same file ONLY once
    # ALL batch processes access the same json file so the result is only stored
    # for the last batch, knowing that the last batch in numbers might not be
    # the last batch actually executed
    if batch_range is not None and batch_range[1] + 1 != len(all_batch_items):
        return

    with open(serialized_json, "w") as f:
        json.dump(node.serialize(), f, indent=2)
