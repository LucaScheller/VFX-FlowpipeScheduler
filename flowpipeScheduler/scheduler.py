import json
import logging
import os
import sys
import uuid
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
    identifier = "FP_IDENTIFIER"
    database_type = "FP_DATABASE_TYPE"
    batch_range = "FP_BATCH_RANGE"


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
        json_data = json.loads(raw_data)
        return json_data


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
        identifier = node.identifier
        json_file_path = JsonDatabase.PATH.format(identifier=node.identifier)
        json_dir_path = os.path.dirname(json_file_path)
        if not os.path.exists(json_dir_path):
            os.makedirs(json_dir_path)
        with open(json_file_path, "w") as json_file:
            json.dump(node.serialize(), json_file, indent=2)
        return identifier

    @staticmethod
    def get(identifier):
        """Retrieve the node behind the given identifier."""
        json_file_path = JsonDatabase.PATH.format(identifier=identifier)
        with open(json_file_path, "r") as json_file:
            json_data = json.load(json_file)
        return json_data


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
COMMAND_INTERPRETER = {
    "python": "/mnt/data/PROJECT/VFX-FlowpipeScheduler/ext/python/bin/python",
    "python_39": "python3.9",
    "python_311": "python3.11",
    "python_312": "python3.12",
    "houdini": "hython",
}


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
    identifier, batch_range=None, database_type=DatabaseType.RedisDatabase
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

    database = get_database(database_type)

    node_identifier = identifier
    node_data = database.get(node_identifier)
    node = INode.deserialize(node_data)

    # Retrieve the upstream output data
    # We have to use the raw json data dict for looking up the connections
    # as a deserialize would need the graph context.
    for name, input_plug in node_data["inputs"].items():
        # Plugs
        for upstream_node_identifier, output_plug in input_plug["connections"].items():
            upstream_node_data = database.get(upstream_node_identifier)
            upstream_node = INode.deserialize(upstream_node_data)
            node.inputs[name].value = upstream_node.outputs[output_plug].value
        # Sub plugs
        for sub_input_plug_idx, sub_input_plug in input_plug["sub_plugs"].items():
            for upstream_node_identifier, output_plug in sub_input_plug[
                "connections"
            ].items():
                upstream_node_data = database.get(upstream_node_identifier)
                upstream_node = INode.deserialize(upstream_node_data)
                # Check four sub plugs
                if "." in output_plug:
                    sub_output_plug, sub_output_plug_idx = output_plug.split(".")
                    node.inputs[name][sub_input_plug_idx].value = upstream_node.outputs[
                        sub_output_plug
                    ][sub_output_plug_idx].value
                else:
                    node.inputs[name][sub_input_plug_idx].value = upstream_node.outputs[
                        output_plug
                    ].value
    # Specifically assign the batch frames here if applicable
    """
    if batch_range is not None:
        all_batch_items = node.inputs["batch_items"]
        node.inputs["batch_items"] = all_batch_items[
            batch_range[0] : batch_range[1] + 1 : batch_range[2]
        ]
    """

    # Evaluate node
    node.evaluate()

    # Store the result back into the same file ONLY once
    # ALL batch processes access the same json file so the result is only stored
    # for the last batch, knowing that the last batch in numbers might not be
    # the last batch actually executed
    """
    if batch_range is not None and batch_range[1] + 1 != len(all_batch_items):
        return
    """
    database.set(node)


# -----------------------------------------------------------------------------
#
# Deadline
#
# -----------------------------------------------------------------------------


class DL_EnvironmentVariables:
    job_pre_script_identifier = "DL_JOB_PRE_SCRIPT_FP_IDENTIFIER"
    job_post_script_identifier = "DL_JOB_POST_SCRIPT_FP_IDENTIFIER"
    task_pre_script_identifier = "DL_TASK_PRE_SCRIPT_FP_IDENTIFIER"
    task_post_script_identifier = "DL_TASK_POST_SCRIPT_FP_IDENTIFIER"


DL_COMMAND_RUNNERS = {
    "runner": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "runner.py"
    ),
    "job_pre_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "job_pre_script_runner.py"
    ),
    "job_post_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "job_post_script_runner.py"
    ),
    "task_pre_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "task_pre_script_runner.py"
    ),
    "task_post_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "task_post_script_runner.py"
    ),
}


def dl_job_script_evaluate_on_farm_through_env(script_type: str):
    """Evaluate on farm by extracting the evaluation data through environment variables.
    Args:
        script_type (str): The job script type, one of ['job_pre', 'job_post', 'task_pre', 'task_post']
    """
    script_type_to_env_var = {
        "job_pre": DL_EnvironmentVariables.job_pre_script_identifier,
        "job_post": DL_EnvironmentVariables.job_post_script_identifier,
        "task_pre": DL_EnvironmentVariables.task_pre_script_identifier,
        "task_post": DL_EnvironmentVariables.task_post_script_identifier,
    }
    identifier = os.environ[script_type_to_env_var[script_type]]
    batch_range = None
    database_type = os.environ[EnvironmentVariables.database_type]
    evaluate_on_farm(identifier, batch_range, database_type)


def dl_convert_graph_to_job(
    connection, graph: Graph, database_type=DatabaseType.RedisDatabase
):
    """Convert the graph to a dict representing a typical render farm job."""

    # Database
    database = get_database(database_type)

    jobs = []
    job_batch_hash = uuid.uuid4().hex
    job_batch_name = "{} ({})".format(graph.name, job_batch_hash)

    # Convert to DL jobs
    node_to_job = {}
    for node in graph.evaluation_sequence:
        db_identifier = database.set(node)
        """
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
        """
        job = DLJobs.Job()
        node_to_job[node.name] = job
        jobs.append(job)
        # General
        job.JobStatus = DLJobs.JobStatus.Active
        job.JobBatchName = job_batch_name
        job.JobName = node.name
        # Env
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.identifier, db_identifier)
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.batch_range, str([]))
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.database_type, database_type)
        # Plugin
        job.JobPlugin = "Command"
        command_executable = COMMAND_INTERPRETER[
            node.metadata.get("interpreter", "python")
        ]
        command_args = DL_COMMAND_RUNNERS["runner"]
        command = "{exe} {args}".format(exe=command_executable, args=command_args)
        job.SetJobPluginInfoKeyValue("Command", command)

    # Submit
    job: DLJobs.Job
    for job in jobs:
        # Dependencies
        node_name = job.JobName
        node = graph[node_name]
        job_dependency_job_ids = []
        for upstream_node in [n.name for n in node.parents]:
            job_dependency_job_ids.append(node_to_job[upstream_node].JobId)
        job.SetJobDependencyIDs(job_dependency_job_ids)
        if job_dependency_job_ids:
            # Dependent jobs are always set to active,
            # since they will be pending anyway.
            job.JobStatus = DLJobs.JobStatus.Active

        jobData, pluginData, auxFilePaths = (
            job.serializeSubmissionCommandlineDictionaries()
        )
        jobWebServiceData = connection.Jobs.SubmitJob(jobData, pluginData, auxFilePaths)
        if not jobWebServiceData:
            raise Exception("Failed to submit job.")
        job.deserializeWebAPI(jobWebServiceData)
