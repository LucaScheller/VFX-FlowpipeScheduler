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

class NodeInputNames:
    batch_items = "batch_items"
    batch_size = "batch_size"
    batch_frame_offset = "batch_frame_offset" # Add a 'cosmetic only' frame offset for UIs.

class NodeInputMetadata:
    job_overrides = "job_overrides"

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
    "python": os.path.join(os.environ["PROJECT"], "VFX-FlowpipeScheduler/ext/python/bin/python"),
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


def evaluate_on_farm_through_env(batch_range=None):
    """Evaluate on farm by extracting the evaluation data through environment variables.
    Args:
        batch_range [tuple(int, int, int)| None]: The batch range.
    """
    identifier = os.environ[EnvironmentVariables.identifier]
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

    # Database
    database = get_database(database_type)

    # Node
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
            # Check for sub plugs (this is a flowpipe bug TODO Report that we can't mix sub plug and plug inputs)
            if "." in output_plug:
                sub_output_plug, sub_output_plug_idx = output_plug.split(".")
                node.inputs[name].value = upstream_node.outputs[sub_output_plug][sub_output_plug_idx].value
            else:
                node.inputs[name].value = upstream_node.outputs[output_plug].value
        # Sub plugs
        for sub_input_plug_idx, sub_input_plug in input_plug["sub_plugs"].items():
            for upstream_node_identifier, output_plug in sub_input_plug[
                "connections"
            ].items():
                upstream_node_data = database.get(upstream_node_identifier)
                upstream_node = INode.deserialize(upstream_node_data)
                # Check for sub plugs
                if "." in output_plug:
                    sub_output_plug, sub_output_plug_idx = output_plug.split(".")
                    node.inputs[name][sub_input_plug_idx].value = upstream_node.outputs[
                        sub_output_plug
                    ][sub_output_plug_idx].value
                else:
                    node.inputs[name][sub_input_plug_idx].value = upstream_node.outputs[
                        output_plug
                    ].value

    # Batch range
    batch_enable = False
    all_batch_items = []
    batch_items_plug = node.inputs.get(NodeInputNames.batch_items, None)
    batch_frame_offset = node.metadata.get(NodeInputNames.batch_frame_offset, 0)
    if batch_range is not None and batch_items_plug is not None:
        if batch_items_plug.value:
            batch_enable = True
            if batch_frame_offset != 0:
                batch_range = list(batch_range) # tuple to list
                batch_range[0] -= batch_frame_offset
                batch_range[1] -= batch_frame_offset
            all_batch_items = node.inputs[NodeInputNames.batch_items].value
            node.inputs[NodeInputNames.batch_items].value = all_batch_items[
                batch_range[0] : batch_range[1] + 1 : batch_range[2]
            ]

    # Evaluate node
    node.evaluate()

    # Batch range restore
    if batch_enable:
        node.inputs[NodeInputNames.batch_items].value = all_batch_items
    # For non-atomic transaction databases, only write the data once.
    # Currently we assume that each batch range cook produces the same output.
    atomic_transaction_databases = [DatabaseType.RedisDatabase]
    if batch_enable and database_type not in atomic_transaction_databases:
        if batch_range[1] + 1 != len(all_batch_items):
            return

    database.set(node)


# -----------------------------------------------------------------------------
#
# Deadline
#
# -----------------------------------------------------------------------------

def dl_get_job_default():
    """Create a job with default farm submission settings.
    Returns:
        DLJobs.Job: The deadline job.
    """
    instance = getattr(dl_get_job_default, "instance", None)
    if not instance:
        job = instance = dl_get_job_default.instance = DLJobs.Job()
        job.JobDepartment = "pipeline"
        job.JobPool = "utility"
        job.JobGroup = "pipeline"
    return instance


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


def dl_send_graph_to_farm(
    connection, graph: Graph, database_type=DatabaseType.RedisDatabase
):
    """Convert the graph to a dict representing a typical render farm job.
    Args:
        connection (DeadlineWebService.DeadlineConnect.DeadlineCon): The deadline webservice connection.
        graph (Graph): The graph.
        database_type (DatabaseType): The database type.
    Returns:
        list[DLJobs.Jobs]: A list of job objects.
    """

    # Database
    database = get_database(database_type)

    jobs = []
    job_batch_hash = uuid.uuid4().hex
    job_batch_name = "{} ({})".format(graph.name, job_batch_hash)

    # Convert to DL jobs
    node_to_job = {}
    for node in graph.evaluation_sequence:
        job = DLJobs.Job()
        node_to_job[node.name] = job
        jobs.append(job)
        # Defaults
        job.applyChangeSet(dl_get_job_default())
        # General
        job.JobStatus = DLJobs.JobStatus.Active
        job.JobBatchName = job_batch_name
        job.JobName = node.name
        # Plugin
        job.JobPlugin = "Command"
        command_executable = COMMAND_INTERPRETER[
            node.metadata.get("interpreter", "python")
        ]
        command_args = [
            DL_COMMAND_RUNNERS["runner"],
            "<FRAME_START>",
            "<FRAME_END>"
        ]
        command = "{exe} {args}".format(exe=command_executable, args=" ".join(command_args))
        job.SetJobPluginInfoKeyValue("Command", command)
        # Batch range (frames/items)
        batch_size_input = node.inputs.get(NodeInputNames.batch_size)
        batch_items_input = node.inputs.get(NodeInputNames.batch_items)
        if batch_items_input and batch_items_input.value:
            batch_item_count = len(batch_items_input.value)
            batch_size = 1
            if batch_size_input:
                batch_size = batch_size_input.value
            # TODO Add a pre job scripts that modify these dynamically.
            batch_frame_offset = node.metadata.get(NodeInputNames.batch_frame_offset, 0)
            batch_frame_start = 0 + batch_frame_offset
            batch_frame_end = batch_item_count + batch_frame_offset
            job.JobFrames = "{}-{}".format(batch_frame_start, batch_frame_end)
            job.JobFramesPerTask = batch_size
            # Enable batch frame dependencies if
            # batch_size and batch item count align. 
            batch_frame_dependency = False
            for upstream_node in node.parents:
                upstream_batch_size_input = upstream_node.inputs.get(NodeInputNames.batch_size)
                upstream_batch_items_input = upstream_node.inputs.get(NodeInputNames.batch_items)
                if upstream_batch_items_input and upstream_batch_items_input.value:
                    upstream_batch_item_count = len(upstream_batch_items_input.value)
                    upstream_batch_size = 1
                    if upstream_batch_size_input:
                        upstream_batch_size = upstream_batch_size_input.value
                    batch_size_matches = batch_size == upstream_batch_size
                    batch_item_count_matches = batch_item_count == upstream_batch_item_count
                    if batch_size_matches and batch_item_count_matches:
                        batch_frame_dependency = True
            job.JobIsFrameDependent = batch_frame_dependency
        # Job Overrides
        job_overrides = node.metadata.get(NodeInputMetadata.job_overrides, None)
        if job_overrides is not None:
            job.applyChangeSet(job_overrides)
            # Force clear this submit only data
            node.metadata.pop(NodeInputMetadata.job_overrides, None)

    # Submit
    job: DLJobs.Job
    job_id_to_job = {}
    for job in jobs:
        # Store job in database
        db_identifier = database.set(node)
        # Env
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.identifier, db_identifier)
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.database_type, database_type)
        # Dependencies
        node_name = job.JobName
        node = graph[node_name]
        job_dependency_job_ids = []
        for upstream_node_name in [n.name for n in node.parents]:
            job_dependency_job_ids.append(node_to_job[upstream_node_name].JobId)
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
        job_id_to_job[job.JobId] = job
        # Align frame offsets of frame dependent dependencies
        # TODO HINT Disable this if the extra query causes a performance hint.
        """
        # Disabled because of deadline bug that only allows offsets under 100.
        if job.JobIsFrameDependent:
            job_frame_first = sorted(job.JobFramesList)[0]
            print(job.JobFramesList)
            for job_dependency in job.JobDependencies:
                dependent_job = job_id_to_job[job_dependency.JobID]
                dependent_job_frame_first = sorted(dependent_job.JobFramesList)[0]
                print(dependent_job.JobFramesList)
                job_dependency.OverrideFrameOffsets=True
                job_dependency.StartOffset = job_frame_first - dependent_job_frame_first
                job_dependency.EndOffset = job_frame_first - dependent_job_frame_first
            connection.Jobs.SaveJob(job.serializeWebAPI())
        """
        # Instead we disable the frame dependency if there is a mis-match
        if job.JobIsFrameDependent:
            job_frame_first = sorted(job.JobFramesList)[0]
            job_dependencies_changed = False
            for job_dependency in job.JobDependencies:
                dependent_job = job_id_to_job[job_dependency.JobID]
                dependent_job_frame_first = sorted(dependent_job.JobFramesList)[0]
                job_dependency.OverrideFrameOffsets=True
                if job_frame_first != dependent_job_frame_first:
                    job_dependency.IgnoreFrameOffsets=True
                    job_dependencies_changed = True
            if job_dependencies_changed:
                state = connection.Jobs.SaveJob(job.serializeWebAPI())
                if state != "Success":
                    raise Exception("Failed to submit job.")
    return jobs