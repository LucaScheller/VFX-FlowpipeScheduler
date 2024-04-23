import json
import logging
import os
import uuid
from tempfile import gettempdir

import redis
from flowpipe import Graph, INode
from deadlineAPI.Deadline import Jobs as DLJobs
from deadlineConfigure.etc.constants import (
    EnvironmentVariables as DL_GlobalEnvVars,
)


# -----------------------------------------------------------------------------
#
# Logging
#
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(os.path.basename(__file__))


# -----------------------------------------------------------------------------
#
# Configuration
#
# -----------------------------------------------------------------------------


class EnvVars:
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
    def set(cls, node: INode):
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
COMMAND_INTERPRETER_DEFAULT = "python"
COMMAND_INTERPRETER_VERSION_DEFAULT = "default"
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
    identifiers = os.environ[EnvVars.identifiers].split(",")
    database_type = os.environ[EnvVars.database_type]
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


class DL_EnvVars:
    job_pre_script_identifiers = "DL_JOB_PRE_SCRIPT_FP_IDENTIFIERS"
    job_post_script_identifiers = "DL_JOB_POST_SCRIPT_FP_IDENTIFIERS"
    job_task_pre_script_identifiers = "DL_JOB_TASK_PRE_SCRIPT_FP_IDENTIFIERS"
    job_task_post_script_identifiers = "DL_JOB_TASK_POST_SCRIPT_FP_IDENTIFIERS"


class DL_NodeInputMetadata(NodeInputMetadata):
    job_overrides = "job_overrides"
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
        "job_pre": DL_EnvVars.job_pre_script_identifiers,
        "job_post": DL_EnvVars.job_post_script_identifiers,
        "job_task_pre": DL_EnvVars.job_task_pre_script_identifiers,
        "job_task_post": DL_EnvVars.job_task_post_script_identifiers,
    }
    identifiers = os.environ[script_type_to_env_var[script_type]].split(",")
    batch_range = batch_range
    database_type = os.environ[EnvVars.database_type]
    evaluate_on_farm(identifiers, batch_range, database_type)


def dl_send_graph_to_farm(
    connection, graph: Graph, optimize=True, database_type=DatabaseType.RedisDatabase
):
    """Convert the graph to a dict representing a typical render farm job.
    Args:
        connection (DeadlineWebService.DeadlineConnect.DeadlineCon): The deadline webservice connection.
        graph (Graph): The graph.
        optimize (bool): Collapse parts of the graph to a single job where possible.
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
    node_id_to_db_id = {}
    node_id_to_job = {}
    node: INode
    for node in graph.evaluation_sequence:
        # Job
        job = DLJobs.Job()
        node_id_to_job[node.identifier] = job
        jobs.append(job)
        # Defaults
        job.applyChangeSet(dl_get_job_default())
        # General
        job.JobBatchName = job_batch_name
        job.sessionData["job_pre_script_nodes"] = []
        job.sessionData["job_task_pre_script_nodes"] = []
        job.sessionData["nodes"] = [node]
        job.sessionData["job_task_post_script_nodes"] = []
        job.sessionData["job_post_script_nodes"] = []
        # Env
        job.SetJobEnvironmentKeyValue(EnvVars.database_type, database_type)
        # Plugin
        job.JobPlugin = "Command"
        node_interpreter = node.metadata.get(DL_NodeInputMetadata.interpreter, COMMAND_INTERPRETER_DEFAULT)
        node_interpreter_version = node.metadata.get(DL_NodeInputMetadata.interpreter_version, COMMAND_INTERPRETER_VERSION_DEFAULT)
        command_executable = COMMAND_INTERPRETER[node_interpreter][node_interpreter_version]
        command_args = [DL_COMMAND_RUNNERS["runner"], "<FRAME_START>", "<FRAME_END>"]
        command = "{exe} {args}".format(exe=command_executable,
                                        args=" ".join(command_args))
        job.SetJobPluginInfoKeyValue("Command", command)
        job.sessionData["node_interpreter_resolved"] = f"{node_interpreter}_{node_interpreter_version}"
        # Limits
        job_resource_limit_interpreter = COMMAND_RESOURCE_LIMITS.get(node_interpreter)
        if job_resource_limit_interpreter:
            job.SetJobLimitGroups([job_resource_limit_interpreter])
        # Batch range (frames/items)
        batch_items_input = node.inputs.get(NodeInputNames.batch_items)
        if batch_items_input and batch_items_input.value:
            batch_item_count = len(batch_items_input.value)
            batch_size_input = node.inputs.get(NodeInputNames.batch_size)
            batch_size = 1
            if batch_size_input and batch_size_input.value:
                batch_size = batch_size_input.value
            # TODO Add a pre job scripts that modify these dynamically.
            batch_frame_offset = node.metadata.get(DL_NodeInputMetadata.batch_frame_offset, 0)
            batch_frame_start = 0 + batch_frame_offset
            batch_frame_end = batch_item_count + batch_frame_offset
            job.JobFrames = "{}-{}".format(batch_frame_start, batch_frame_end)
            job.JobFramesPerTask = batch_size
        # Job Overrides
        job_overrides = node.metadata.get(DL_NodeInputMetadata.job_overrides, None)
        if job_overrides is not None:
            job.applyChangeSet(job_overrides)
            # Force clear this submit only data
            node.metadata.pop(DL_NodeInputMetadata.job_overrides)
        # Store node in database (must be done as a last step, so that we have the configured data)
        db_id = database.set(node)
        node_id_to_db_id[node.identifier] = db_id

    # Graph optimization / Job pre/post (task) scripts
    """Order of operations:
    1. Validate pre/post job script metadata and remove if it can't be represented as a deadline dependency
    2. Collapse "collapse-able" parts of the graph (node that ony have 1 parent/child node and match configuration (interpreter/batching))
    3. Honor pre/post script tagging of nodes:
        - Collapse jobs via: job pre -> task pre -> job <- task post <- job post
        - We allow chaining multiple pre/post scripts of the same same type.
    4. Set necessary job data
    """

    def get_batch_values(node):
        batch_active = False
        batch_items_input = node.inputs.get(NodeInputNames.batch_items)
        batch_size = None
        batch_item_count = None
        if batch_items_input and batch_items_input.value:
            batch_active = True
            batch_item_count = len(batch_items_input.value)
            batch_size_input = node.inputs.get(NodeInputNames.batch_size)
            batch_size = 1
            if batch_size_input and batch_size_input.value:
                batch_size = batch_size_input.value
        return batch_active, batch_size, batch_item_count

    def validate_job_script(node, direction):
        """Check if the node is script attachable.
        Args:
            node (Node): The node.
            direction (str): The direction to traverse to, either "parents" or "children".
        Returns:
            bool: The state. If False, node can't be run in a job script.
        """
        # Compare node
        compare_nodes = getattr(node, direction)
        if not compare_nodes:
            return False
        if not len(compare_nodes) == 1:
            return False
        compare_node = list(compare_nodes)[0]
        # Interpreter
        node_interpreter = node.metadata.get(DL_NodeInputMetadata.interpreter, "python")
        node_interpreter_version = node.metadata.get(DL_NodeInputMetadata.interpreter_version, "default")
        if node_interpreter != "python" or node_interpreter_version != "default":
            return False
        # Script Type
        node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        compare_node_job_script_type = compare_node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        if compare_node_job_script_type is not None:
            if compare_node_job_script_type != node_job_script_type:
                return False
        # Batch range
        batch_active, batch_size, batch_item_count = get_batch_values(node)
        # If batching is active, we enforce to attach it as a pre/post task
        # script or otherwise fallback to its own job. 
        # While we could also run it as a single job pre/post script, if the batch config
        # doesn't match, we would loose the parallelizing nature (that is why we enforce it).
        # Logic:
        # - If active node is without batching, attach it as a pre/post job script
        # - If active node is with batching, attach it as pre/post task script if compare node is also batched
        # - If active node is with batching and the compare node isn't, don't attach as a job/task script
        if batch_active:
            compare_batch_active, compare_batch_size, compare_batch_item_count = get_batch_values(compare_node)
            if not compare_batch_active:
                return False
            if batch_size != compare_batch_size:
                return False
            if batch_item_count != compare_batch_item_count:
                return False
        return True
        
    def validate_job_collapse(node):
        """Check if the node is script attachable.
        Args:
            node (Node): The node.
        Returns:
            bool: The state. If True, node can be "merged" into the downstream node.
        """
        # Child node
        children_nodes = node.children
        if not children_nodes:
            return False
        if not len(children_nodes) == 1:
            return False
        child_node = list(children_nodes)[0]
        child_node_parents = child_node.parents
        if len(child_node_parents) != 1:
            return False
        # Optimize Override
        node_graph_optimize = node.metadata.get(DL_NodeInputMetadata.graph_optimize, True)
        child_node_graph_optimize = child_node.metadata.get(DL_NodeInputMetadata.graph_optimize, True)
        if not node_graph_optimize or not child_node_graph_optimize:
            return False
        # Interpreter
        node_interpreter = node.metadata.get(DL_NodeInputMetadata.interpreter, COMMAND_INTERPRETER_DEFAULT)
        node_interpreter_version = node.metadata.get(DL_NodeInputMetadata.interpreter_version, COMMAND_INTERPRETER_VERSION_DEFAULT)
        child_node_interpreter = child_node.metadata.get(DL_NodeInputMetadata.interpreter, COMMAND_INTERPRETER_DEFAULT)
        child_node_interpreter_version = child_node.metadata.get(DL_NodeInputMetadata.interpreter_version, COMMAND_INTERPRETER_VERSION_DEFAULT)
        if node_interpreter != child_node_interpreter or node_interpreter_version != child_node_interpreter_version:
            return False
        # Script Type
        node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        child_node_job_script_type = child_node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        if node_job_script_type is not None or child_node_job_script_type is not None:
            return False
        # Batch range
        batch_active, batch_size, batch_item_count = get_batch_values(node)
        child_batch_active, child_batch_size, child_batch_item_count = get_batch_values(child_node)
        if batch_active != child_batch_active:
            return False
        if batch_size != child_batch_size:
                return False
        if batch_item_count != child_batch_item_count:
            return False
        return True

    # Step 1: Validate script type
    for job in jobs:
        node = job.sessionData["nodes"][0]
        # Validate direct single parent node
        node_job_script_type =  node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        if node_job_script_type is not None:
            direction = {
                DL_JobScriptType.pre: "children",
                DL_JobScriptType.post: "parents"
            }
            # Validate direct single upstream/downstream node
            if not validate_job_script(node, direction[node_job_script_type]):
                LOG.debug("Removing '{}' job script type from node '{}' as it doesn't meet "
                          "the necessary requirements.".format(node_job_script_type, node.name))
                node.metadata.pop(DL_NodeInputMetadata.job_script_type, None)
    # Step 2: Collapse graph where possible from "left to right"
    if optimize:
        optimized_jobs = []
        for job in jobs:
            nodes = job.sessionData["nodes"]
            node = job.sessionData["nodes"][-1]
            if not validate_job_collapse(node):
                optimized_jobs.append(job)
                continue
            # Move node to child job
            child_node: INode
            child_node = list(node.children)[0]
            child_job: DLJobs.Job
            child_job = node_id_to_job[child_node.identifier]
            child_job.sessionData["nodes"] = nodes + child_job.sessionData["nodes"]
            node_id_to_job[node.identifier] = child_job
            # TODO Here we could decide to also merge certain job settings
        jobs = optimized_jobs
    # Step 3: Collapse graph where possible using job/task pre/post scripts
    submit_jobs = []
    for job in jobs:
        # Through our step 1 and 2 validation,
        # script type jobs only have 1 node.
        node = job.sessionData["nodes"][0]
        node_job_script_type = node.metadata.get(DL_NodeInputMetadata.job_script_type, None)
        if node_job_script_type is None:
            submit_jobs.append(job)
            continue
        if node_job_script_type == DL_JobScriptType.pre:
            # Move node to child job
            child_node: INode
            child_node = list(node.children)[0]
            child_job: DLJobs.Job
            child_job = node_id_to_job[child_node.identifier]
            # Chain nodes during job iteration
            batch_active, batch_size, batch_item_count = get_batch_values(node)
            node_key = "job_pre_script_nodes" if not batch_active else "job_task_pre_script_nodes"
            script_nodes = [n for n in job.sessionData[node_key]]
            child_script_nodes = [n for n in child_job.sessionData[node_key]]
            combined_script_nodes = script_nodes + child_script_nodes + [node]
            child_job.sessionData[node_key] = combined_script_nodes
            for n in combined_script_nodes:
                node_id_to_job[n.identifier] = child_job
        if node_job_script_type == DL_JobScriptType.post:
            # Move node to parent job
            parent_node = list(node.parents)[0]
            parent_job: DLJobs.Job
            parent_job = node_id_to_job[parent_node.identifier]
            # Chain nodes during job iteration
            batch_active, batch_size, batch_item_count = get_batch_values(node)
            node_key = "job_post_script_nodes" if not batch_active else "job_task_post_script_nodes"
            script_nodes = [n for n in job.sessionData[node_key]]
            parent_script_nodes = [n for n in parent_job.sessionData[node_key]]
            combined_script_nodes = [node] + parent_script_nodes + script_nodes
            parent_job.sessionData[node_key] = combined_script_nodes
            for n in combined_script_nodes:
                node_id_to_job[n.identifier] = parent_job
    jobs = submit_jobs
    # Step 4: Serialize to db and configure jobs to match collapse state
    for job in jobs:
        job_pre_script_nodes = job.sessionData["job_pre_script_nodes"]
        job_task_pre_script_nodes = job.sessionData["job_task_pre_script_nodes"]
        job_nodes = job.sessionData["nodes"]
        job_task_post_script_nodes = job.sessionData["job_task_post_script_nodes"]
        job_post_script_nodes = job.sessionData["job_post_script_nodes"]
        # Label
        all_nodes = job_pre_script_nodes + job_task_pre_script_nodes + job_nodes + job_task_post_script_nodes + job_post_script_nodes
        job.sessionData["all_nodes"] = all_nodes
        job_name = []
        for n in all_nodes:
            node_name = n.name
            if n.graph != graph:
                node_name = "{} ({})".format(n.name, n.graph.name)
            job_name.append(node_name)
        job_name = ", ".join(job_name)
        job.JobName = job_name
        # Scripts
        if job_pre_script_nodes:
            identifiers = [node_id_to_db_id[n.identifier] for n in job_pre_script_nodes]
            job.JobPreJobScript = DL_COMMAND_RUNNERS["job_pre_script"]
            job.SetJobEnvironmentKeyValue(DL_EnvVars.job_pre_script_identifiers,
                                          ",".join(identifiers))
        if job_task_pre_script_nodes:
            identifiers = [node_id_to_db_id[n.identifier] for n in job_task_pre_script_nodes]
            job.JobPreTaskScript = DL_COMMAND_RUNNERS["job_task_pre_script"]
            job.SetJobEnvironmentKeyValue(DL_EnvVars.job_task_pre_script_identifiers,
                                          ",".join(identifiers))
        identifiers = [node_id_to_db_id[n.identifier] for n in job_nodes]
        job.SetJobEnvironmentKeyValue(EnvVars.identifiers, ",".join(identifiers))
        if job_task_post_script_nodes:
            identifiers = [node_id_to_db_id[n.identifier] for n in job_task_post_script_nodes]
            job.JobPostTaskScript = DL_COMMAND_RUNNERS["job_task_post_script"]
            job.SetJobEnvironmentKeyValue(DL_EnvVars.job_task_post_script_identifiers,
                                          ",".join(identifiers))
        if job_post_script_nodes:
            identifiers = [node_id_to_db_id[n.identifier] for n in job_post_script_nodes]
            job.JobPostJobScript = DL_COMMAND_RUNNERS["job_post_script"]
            job.SetJobEnvironmentKeyValue(DL_EnvVars.job_post_script_identifiers,
                                          ",".join(identifiers))
        # Redis
        if database_type == DatabaseType.RedisDatabase:
            job_redis_keys = [
                node_id_to_db_id[n.identifier] for n in all_nodes
            ]
            job.SetJobEnvironmentKeyValue(
                DL_GlobalEnvVars.JOB_REDIS_KEYS, ",".join(job_redis_keys)
            )
            job.JobEventOptIns = job.JobEventOptIns + ["Redis"]

    # Submit
    job: DLJobs.Job
    job_id_to_job = {}
    for job in jobs:
        # Dependencies
        node = job.sessionData["all_nodes"][0]
        dependency_jobs = []
        for upstream_node in node.parents:
            upstream_job = node_id_to_job[upstream_node.identifier]
            dependency_jobs.append(upstream_job)
        if dependency_jobs:
            job.SetJobDependencyIDs([j.JobId for j in dependency_jobs])
            # Dependent jobs are always set to active,
            # since they will be pending anyway.
            job.JobStatus = DLJobs.JobStatus.Active
        # Frame dependencies
        frame_dependencies_capable = True
        if job.JobPreJobScript:
            frame_dependencies_capable = False
        if frame_dependencies_capable:
            for dep_job in dependency_jobs:
                if len(dep_job.JobFramesList) != len(job.JobFramesList):
                    frame_dependencies_capable = False
                if dep_job.JobFramesPerTask != job.JobFramesPerTask:
                    frame_dependencies_capable = False
                if dep_job.JobPostJobScript:
                    frame_dependencies_capable = False
        if frame_dependencies_capable:
            job.JobIsFrameDependent = True
        # Submission data
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
                    raise Exception("Failed to edit job.")
    return jobs
