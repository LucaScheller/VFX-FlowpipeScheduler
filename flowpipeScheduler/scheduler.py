import json
import logging
import os
import sys
import uuid
from tempfile import gettempdir

import redis
from flowpipe import Graph, INode, Node
from deadlineAPI.Deadline import Jobs as DLJobs
from deadlineConfigure.etc.constants import (
    EnvironmentVariables as DL_GlobalEnvironmentVariables,
)

# -----------------------------------------------------------------------------
#
# Configuration
#
# -----------------------------------------------------------------------------


class EnvironmentVariables:
    identifiers = "FP_IDENTIFIERS"
    database_type = "FP_DATABASE_TYPE"


class NodeInputNames:
    batch_items = "batch_items"
    batch_size = "batch_size"


class NodeInputMetadata:
    batch_frame_offset = (
        "batch_frame_offset"  # Add a 'cosmetic only' frame offset for UIs.
    )
    interpreter = "interpreter"
    interpreter_version = "interpreter_version"
    graph_optimize = "graph_optimize"


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
    "python": {
        "default": os.path.join(
            os.environ["PROJECT"], "VFX-FlowpipeScheduler/ext/python/bin/python"
        ),
        "python_39": "python3.9",
        "python_311": "python3.11",
        "python_312": "python3.12",
    },
    "houdini": {
        "default": "/opt/hfs20.0/bin/hython",
        "houdini_190": "/opt/hfs19.0/bin/hython",
        "houdini_195": "/opt/hfs19.5/bin/hython",
        "houdini_20": "/opt/hfs20.0/bin/hython",
    },
}

COMMAND_RESOURCE_LIMITS = {"houdini": "houdini", "python": "cache"}


# -----------------------------------------------------------------------------
#
# Evaluation
#
# -----------------------------------------------------------------------------


def evaluate_on_farm_through_env(batch_range: tuple[int, int, int]):
    """Evaluate on farm by extracting the evaluation data through environment variables.
    Args:
        batch_range [tuple(int, int, int)| None]: The batch range.
    """
    identifiers = os.environ[EnvironmentVariables.identifiers].split(",")
    database_type = os.environ[EnvironmentVariables.database_type]
    evaluate_on_farm(identifiers, batch_range, database_type)


def evaluate_on_farm(
    identifiers: list[str], batch_range: tuple[int, int, int], database_type: DatabaseType.RedisDatabase
):
    """Evaluate the node(s) based on the given identifier(s).
    Args:
        identifiers (list[str]): A list of identifiers.
        batch_range (list[any]): A list of elements to batch.
        database_type (DatabaseType): A database type.
    """
    for identifier in identifiers:
        evaluate_on_farm_kernel(
            identifier, batch_range=batch_range, database_type=database_type
        )


def evaluate_on_farm_kernel(
    identifier: str, batch_range: tuple[int, int, int], database_type: DatabaseType.RedisDatabase
):
    """Evaluate the node based on the given identifier.
    Notes:
        1. Deserialize the node from the database
        2. Collect any input values from any upstream dependencies
        3. Evaluate the node
        4. Serialize the node back to the database
    Args:
        identifier (str): A identifier.
        batch_range (tuple[int, int, int]|None): A list of elements to batch.
        database_type (DatabaseType): A database type.
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
                node.inputs[name].value = upstream_node.outputs[sub_output_plug][
                    sub_output_plug_idx
                ].value
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
    batch_frame_offset = node.metadata.get(DL_NodeInputMetadata.batch_frame_offset, 0)
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
        job.JobPool = "stations"
        job.JobGroup = "pipeline"
    return instance


class DL_EnvironmentVariables:
    job_pre_script_identifiers = "DL_JOB_PRE_SCRIPT_FP_IDENTIFIERS"
    job_post_script_identifiers = "DL_JOB_POST_SCRIPT_FP_IDENTIFIERS"
    job_task_pre_script_identifiers = "DL_JOB_TASK_PRE_SCRIPT_FP_IDENTIFIERS"
    job_task_post_script_identifiers = "DL_JOB_TASK_POST_SCRIPT_FP_IDENTIFIERS"


class DL_NodeInputMetadata(NodeInputMetadata):
    job_overrides = "job_overrides"
    job_optimize_graph = "job_optimize_graph"
    job_script_type = "job_script_type"


class DL_JobScriptType:
    pre = "pre"
    post = "post"


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
    "job_task_pre_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "job_task_pre_script_runner.py"
    ),
    "job_task_post_script": os.path.join(
        os.path.dirname(__file__), "deadline", "runners", "job_task_post_script_runner.py"
    ),
}


def dl_job_script_evaluate_on_farm_through_env(script_type: str, batch_range: tuple[int, int, int]):
    """Evaluate on farm by extracting the evaluation data through environment variables.
    Args:
        script_type (str): The job script type, one of ['job_pre', 'job_post', 'job_task_pre', 'job_task_post']
        batch_range (list[any]): A list of elements to batch.
    """
    script_type_to_env_var = {
        "job_pre": DL_EnvironmentVariables.job_pre_script_identifiers,
        "job_post": DL_EnvironmentVariables.job_post_script_identifiers,
        "job_task_pre": DL_EnvironmentVariables.job_task_pre_script_identifiers,
        "job_task_post": DL_EnvironmentVariables.job_task_post_script_identifiers,
    }
    identifiers = os.environ[script_type_to_env_var[script_type]].split(",")
    batch_range = batch_range
    database_type = os.environ[EnvironmentVariables.database_type]
    evaluate_on_farm(identifiers, batch_range, database_type)


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
    node_name_to_job = {}
    node_name_to_db_identifier = {}
    node_to_job_pre_script_nodes = {}
    for node in graph.evaluation_sequence:
        ### CONFIG START ###
        # Job
        job = DLJobs.Job()
        # Defaults
        job.applyChangeSet(dl_get_job_default())
        # General
        job.JobStatus = DLJobs.JobStatus.Active
        job.JobBatchName = job_batch_name
        job.JobName = node.name
        job.sessionData["node_name"] = node.name
        # Plugin
        job.JobPlugin = "Command"
        node_interpreter = node.metadata.get(DL_NodeInputMetadata.interpreter, "python")
        node_interpreter_version = node.metadata.get(DL_NodeInputMetadata.interpreter_version, "default")
        command_executable = COMMAND_INTERPRETER[node_interpreter][
            node_interpreter_version
        ]
        command_args = [DL_COMMAND_RUNNERS["runner"], "<FRAME_START>", "<FRAME_END>"]
        command = "{exe} {args}".format(
            exe=command_executable, args=" ".join(command_args)
        )
        job.SetJobPluginInfoKeyValue("Command", command)
        # Limits
        job_resource_limit_interpreter = COMMAND_RESOURCE_LIMITS.get(node_interpreter)
        if job_resource_limit_interpreter:
            job.SetJobLimitGroups([job_resource_limit_interpreter])
        # Batch range (frames/items)
        batch_items_input = node.inputs.get(NodeInputNames.batch_items)
        batch_item_count = -1
        batch_size_input = node.inputs.get(NodeInputNames.batch_size)
        batch_size = 1
        batch_frame_offset = 0
        batch_enabled = False
        if batch_items_input and batch_items_input.value:
            batch_enabled = True # Used by job pre script calculation
            batch_item_count = len(batch_items_input.value)
            if batch_size_input:
                batch_size = batch_size_input.value
            # TODO Add a pre job scripts that modify these dynamically.
            batch_frame_offset = node.metadata.get(DL_NodeInputMetadata.batch_frame_offset, 0)
            batch_frame_start = 0 + batch_frame_offset
            batch_frame_end = batch_item_count + batch_frame_offset
            job.JobFrames = "{}-{}".format(batch_frame_start, batch_frame_end)
            job.JobFramesPerTask = batch_size
            # Enable batch frame dependencies if
            # batch_size and batch item count align.
            batch_frame_dependency = False
            for upstream_node in node.parents:
                upstream_batch_size_input = upstream_node.inputs.get(
                    NodeInputNames.batch_size
                )
                upstream_batch_items_input = upstream_node.inputs.get(
                    NodeInputNames.batch_items
                )
                if upstream_batch_items_input and upstream_batch_items_input.value:
                    upstream_batch_item_count = len(upstream_batch_items_input.value)
                    upstream_batch_size = 1
                    if upstream_batch_size_input:
                        upstream_batch_size = upstream_batch_size_input.value
                    batch_size_matches = batch_size == upstream_batch_size
                    batch_item_count_matches = (
                        batch_item_count == upstream_batch_item_count
                    )
                    if batch_size_matches and batch_item_count_matches:
                        batch_frame_dependency = True
            job.JobIsFrameDependent = batch_frame_dependency
        # Job Overrides
        job_overrides = node.metadata.get(DL_NodeInputMetadata.job_overrides, None)
        if job_overrides is not None:
            job.applyChangeSet(job_overrides)
            # Force clear this submit only data
            node.metadata.pop(DL_NodeInputMetadata.job_overrides, None)
        # Store node in database
        db_identifier = database.set(node)
        node_name_to_db_identifier[node.name] = db_identifier
        db_identifiers = [db_identifier]
        # Env
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.identifiers, ",".join(db_identifiers))
        job.SetJobEnvironmentKeyValue(EnvironmentVariables.database_type, database_type)
        if database_type == DatabaseType.RedisDatabase:
            job.JobEventOptIns = job.JobEventOptIns + ["Redis"]
            job.SetJobEnvironmentKeyValue(
                DL_GlobalEnvironmentVariables.JOB_REDIS_KEYS, ",".join(db_identifiers)
            )
        ### CONFIG END ###
        node_name_to_job[node.name] = job
        jobs.append(job)
        # We use 'continue' below, so make sure all configuration related edits are done above.
        # Below we re-arrange the graph to be optimized for submission.

        # Job Pre/Post Task Scripts (Attach node to previous or next job if possible based on node config)
        node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        if node_job_script_type is not None:
            if node_job_script_type == DL_JobScriptType.pre:
                # Validate
                ## Children
                node_children = node.children
                if not node_children:
                    continue
                if not len(node_children) == 1:
                    continue
                child_node = list(node_children)[0]
                ## Script Type
                child_node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
                if child_node_job_script_type is not None:
                    if child_node_job_script_type != node_job_script_type:
                        continue
                ## Interpreter
                child_node_interpreter = child_node.metadata.get(DL_NodeInputMetadata.interpreter, "python")
                child_node_interpreter_version = child_node.metadata.get(DL_NodeInputMetadata.interpreter_version, "default")
                if not child_node_interpreter == "python" or not node_interpreter == "python":
                    continue
                if not child_node_interpreter_version == "default" or not node_interpreter_version == "default":
                    continue
                """# Deadline only supports python scripts in its native interpreter
                if node_interpreter != child_node_interpreter:
                    continue
                if node_interpreter_version != child_node_interpreter_version:
                    continue
                """
                ## Batch range
                script_batch_enabled = False
                child_batch_items_input = child_node.inputs.get(NodeInputNames.batch_items)
                if (batch_items_input and batch_items_input.value) and (child_batch_items_input and child_batch_items_input.value):
                    child_batch_item_count = len(batch_items_input.value)
                    if batch_item_count != child_batch_item_count:
                        continue
                    child_batch_size_input = child_node.inputs.get(NodeInputNames.batch_size)
                    child_batch_size = 1
                    if child_batch_size_input:
                        child_batch_size = child_batch_size_input.value
                    if batch_size != child_batch_size:
                        continue
                    child_batch_frame_offset = child_node.metadata.get(DL_NodeInputMetadata.batch_frame_offset, 0)
                    if batch_frame_offset != child_batch_frame_offset:
                        continue
                    script_batch_enabled = True
                ## Tag for mapping
                current_node_job_pre_script_nodes = node_to_job_pre_script_nodes.pop(node.name, [])
                node_to_job_pre_script_nodes[child_node.name] = current_node_job_pre_script_nodes + [node]
                node_name_to_job.pop(node.name)
                jobs.pop(-1)
                continue
            if node_job_script_type == DL_JobScriptType.post:
                # Validate
                ## Parents
                node_parents = node.parents
                if not node_parents:
                    continue
                if not len(node_parents) == 1:
                    continue
                parent_node = list(node_parents)[0]
                ## Script Type
                parent_node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
                if parent_node_job_script_type is not None:
                    if parent_node_job_script_type != node_job_script_type:
                        continue
                ## Interpreter
                parent_node_interpreter = parent_node.metadata.get(DL_NodeInputMetadata.interpreter, "python")
                parent_node_interpreter_version = parent_node.metadata.get(DL_NodeInputMetadata.interpreter_version, "default")
                if not parent_node_interpreter == "python" or not node_interpreter == "python":
                    continue
                if not parent_node_interpreter_version == "default" or not node_interpreter_version == "default":
                    continue
                """# Deadline only supports python scripts in its native interpreter
                if node_interpreter != parent_node_interpreter:
                    continue
                if node_interpreter_version != parent_node_interpreter_version:
                    continue
                """
                ## Batch range
                batch_enabled = False
                parent_batch_items_input = parent_node.inputs.get(NodeInputNames.batch_items)
                if (batch_items_input and batch_items_input.value) and (parent_batch_items_input and parent_batch_items_input.value):
                    parent_batch_item_count = len(batch_items_input.value)
                    if batch_item_count != parent_batch_item_count:
                        continue
                    parent_batch_size_input = parent_node.inputs.get(NodeInputNames.batch_size)
                    parent_batch_size = 1
                    if parent_batch_size_input:
                        parent_batch_size = parent_batch_size_input.value
                    if batch_size != parent_batch_size:
                        continue
                    parent_batch_frame_offset = parent_node.metadata.get(DL_NodeInputMetadata.batch_frame_offset, 0)
                    if batch_frame_offset != parent_batch_frame_offset:
                        continue
                    script_batch_enabled = True
                ## Parent job
                parent_job: DLJobs.Job
                parent_job = node_name_to_job[parent_node.name]
                # Re-direct node
                node_name_to_job[node.name] = parent_job
                jobs.pop(-1)
                parent_job_name_expanded = parent_job.JobName.split(", ")
                parent_job.JobName = ", ".join(parent_job_name_expanded + [node.name])
                post_script_token = DL_EnvironmentVariables.job_post_script_identifiers if not script_batch_enabled else DL_EnvironmentVariables.job_task_post_script_identifiers
                parent_post_script_db_identifiers = parent_job.GetJobEnvironmentKeyValue(post_script_token) or ""
                parent_post_script_db_identifiers = [i for i in parent_post_script_db_identifiers.split(",") if i]
                combined_post_script_db_identifiers = parent_post_script_db_identifiers + db_identifiers
                parent_job.SetJobEnvironmentKeyValue(post_script_token, ",".join(combined_post_script_db_identifiers))
                if not script_batch_enabled:
                    parent_job.JobPostJobScript = DL_COMMAND_RUNNERS["job_post_script"]
                else:
                    parent_job.JobPostTaskScript = DL_COMMAND_RUNNERS["job_task_post_script"]
                if database_type == DatabaseType.RedisDatabase:
                    parent_job_redis_keys = parent_job.GetJobEnvironmentKeyValue(DL_GlobalEnvironmentVariables.JOB_REDIS_KEYS).split(",")
                    parent_job_redis_keys = list(set(parent_job_redis_keys + combined_post_script_db_identifiers))
                    parent_job.SetJobEnvironmentKeyValue(
                        DL_GlobalEnvironmentVariables.JOB_REDIS_KEYS, ",".join(parent_job_redis_keys)
                    )
        # Pre-pend pre-scripts collected above.
        job_pre_scripts_nodes = node_to_job_pre_script_nodes.get(node.name)
        if job_pre_scripts_nodes:
            parent_node_names = []
            parent_node_identifiers = []
            for parent_node in job_pre_scripts_nodes:
                node_name_to_job[parent_node.name] = job
                parent_node_names.append(parent_node.name)
                parent_node_identifiers.append(node_name_to_db_identifier[parent_node.name])
            job_name_expanded = job.JobName.split(", ")
            job.JobName = ", ".join(parent_node_names + job_name_expanded)
            pre_script_token = DL_EnvironmentVariables.job_pre_script_identifiers if not batch_enabled else DL_EnvironmentVariables.job_task_pre_script_identifiers
            node_pre_script_db_identifiers = job.GetJobEnvironmentKeyValue(pre_script_token) or "" # This should always be empty.
            node_pre_script_db_identifiers = [i for i in node_pre_script_db_identifiers.split(",") if i]
            combined_pre_script_db_identifiers = node_pre_script_db_identifiers + parent_node_identifiers
            job.SetJobEnvironmentKeyValue(pre_script_token, ",".join(combined_pre_script_db_identifiers))
            if not batch_enabled:
                job.JobPreJobScript = DL_COMMAND_RUNNERS["job_pre_script"]
            else:
                job.JobPreTaskScript = DL_COMMAND_RUNNERS["job_task_pre_script"]
            if database_type == DatabaseType.RedisDatabase:
                job_redis_keys = job.GetJobEnvironmentKeyValue(DL_GlobalEnvironmentVariables.JOB_REDIS_KEYS).split(",")
                job_redis_keys = list(set(job_redis_keys + combined_pre_script_db_identifiers))
                job.SetJobEnvironmentKeyValue(
                    DL_GlobalEnvironmentVariables.JOB_REDIS_KEYS, ",".join(job_redis_keys)
                )

    # Submit
    job: DLJobs.Job
    job_id_to_job = {}
    for job in jobs:
        # Dependencies
        node_name = job.sessionData["node_name"]
        node = graph[node_name]
        job_dependency_job_ids = []
        for upstream_node_name in [n.name for n in node.parents]:
            upstream_job = node_name_to_job[upstream_node_name]
            # Check for pre-script "job collapsing"
            if upstream_job == job:
                for upstream_node in node.upstream_nodes:
                    upstream_job = node_name_to_job[upstream_node.name]
                    if upstream_job != job:
                        job_dependency_job_ids.append(upstream_job.JobId)
                        break
            else:
                job_dependency_job_ids.append(upstream_job.JobId)
        job.SetJobDependencyIDs(job_dependency_job_ids)
        if job_dependency_job_ids:
            # Dependent jobs are always set to active,
            # since they will be pending anyway.
            job.JobStatus = DLJobs.JobStatus.Active
        job_data, plugin_data, aux_file_paths = (
            job.serializeSubmissionCommandlineDictionaries()
        )
        job_webservice_data = connection.Jobs.SubmitJob(
            job_data, plugin_data, aux_file_paths
        )
        if not job_webservice_data:
            raise Exception("Failed to submit job.")
        job.deserializeWebAPI(job_webservice_data)
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
                job_dependency.OverrideFrameOffsets = True
                if job_frame_first != dependent_job_frame_first:
                    job_dependency.IgnoreFrameOffsets = True
                    job_dependencies_changed = True
            if job_dependencies_changed:
                state = connection.Jobs.SaveJob(job.serializeWebAPI())
                if state != "Success":
                    raise Exception("Failed to submit job.")
    return jobs
