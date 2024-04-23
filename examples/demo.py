import DeadlineWebService.DeadlineConnect as Connect
from deadlineAPI.Deadline.Jobs import Job

from flowpipe import Graph
from flowpipeScheduler import scheduler
from flowpipeScheduler.scheduler import DL_JobScriptType, DL_NodeInputMetadata
from nodes import *


def dl_flowpipe_example_vfx_submit(connection):
    # Create graph
    graph = Graph(name="Rendering")
    # Create nodes
    scene_descr_node = GenerateSceneDescriptionNode(graph=graph,
                                                    name="Generate Scene Description")
    render_image_node_1 = RenderImageNode(graph=graph,
                                          renderpass="characterA",
                                          name="Render Image A")
    render_image_node_2 = RenderImageNode(graph=graph,
                                          renderpass="characterB",
                                          name="Render Image B")
    
    validate_image_node_1 = ValidateImageNode(graph=graph,
                                              name="Validate Image A")
    validate_image_node_2 = ValidateImageNode(graph=graph,
                                              name="Validate Image B")

    notify_user_node_1 = NotifyUserNode(graph=graph,
                                        name="Notifying Users",
                                        user_names=["lucsch", "pipeline"])

    multipart_convert_node_1 = MultipartConvertImageNode(graph=graph,
                                                         name="Multipart Convert A")

    denoise_image_node_1 = DenoiseImageNode(graph=graph,
                                            name="Denoise Image A")
    
    update_db_node = UpdateDatabaseNode(graph=graph,
                                        name="Update Database")

    # Node connections
    scene_descr_node.outputs["sceneDescription_file_path"].connect(
        render_image_node_1.inputs["sceneDescription_file_path"]
    )
    scene_descr_node.outputs["sceneDescription_file_path"].connect(
        render_image_node_2.inputs["sceneDescription_file_path"]
    )
    ## We can use '>>'/'<<' as syntactic sugar instead of connect/disconnect
    render_image_node_1.outputs["image_file_path"] >> validate_image_node_1.inputs["image_file_path"]
    render_image_node_2.outputs["image_file_path"] >> validate_image_node_2.inputs["image_file_path"]
    
    if False:
        validate_image_node_1.outputs["image_file_path"] >> notify_user_node_1.inputs["image_file_path"]
        notify_user_node_1.outputs["image_file_path"] >> multipart_convert_node_1.inputs["image_file_path"]
        multipart_convert_node_1.outputs["image_file_path"] >> denoise_image_node_1.inputs["image_file_path"]
    else:
        graph.delete_node(notify_user_node_1)
        validate_image_node_1.outputs["image_file_path"] >> multipart_convert_node_1.inputs["image_file_path"]
        multipart_convert_node_1.outputs["image_file_path"] >> denoise_image_node_1.inputs["image_file_path"]

    validate_image_node_2.outputs["image_file_path"] >> update_db_node.inputs["image_file_paths"]["0"]
    denoise_image_node_1.outputs["image_file_path"] >> update_db_node.inputs["image_file_paths"]["1"]
    denoise_image_node_1.outputs["denoise_image_file_path"] >> update_db_node.inputs["image_file_paths"]["2"]

    # Job configuration
    job_scene_descr_overrides = Job()
    job_scene_descr_overrides.JobPriority = 100
    scene_descr_node.metadata[DL_NodeInputMetadata.job_overrides] = job_scene_descr_overrides

    job_batch_overrides = Job()
    job_batch_overrides.JobPriority = 50
    job_batch_items = list(range(1001, 1020))
    job_batch_size = 5

    render_image_node_1.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides
    render_image_node_2.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides
    validate_image_node_1.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides
    validate_image_node_2.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides
    multipart_convert_node_1.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides
    denoise_image_node_1.metadata[DL_NodeInputMetadata.job_overrides] = job_batch_overrides

    render_image_node_1.inputs["batch_items"].value = job_batch_items
    render_image_node_2.inputs["batch_items"].value = job_batch_items
    validate_image_node_1.inputs["batch_items"].value = job_batch_items
    validate_image_node_2.inputs["batch_items"].value = job_batch_items
    multipart_convert_node_1.inputs["batch_items"].value = job_batch_items
    denoise_image_node_1.inputs["batch_items"].value = job_batch_items
    render_image_node_1.inputs["batch_size"].value = job_batch_size
    render_image_node_2.inputs["batch_size"].value = job_batch_size
    validate_image_node_1.inputs["batch_size"].value = job_batch_size
    validate_image_node_2.inputs["batch_size"].value = job_batch_size
    multipart_convert_node_1.inputs["batch_size"].value = job_batch_size
    denoise_image_node_1.inputs["batch_size"].value = job_batch_size

    render_image_node_1.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001
    render_image_node_2.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001
    validate_image_node_1.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001
    validate_image_node_2.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001
    multipart_convert_node_1.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001
    denoise_image_node_1.metadata[DL_NodeInputMetadata.batch_frame_offset] = 1001

    # Optimize Example
    optimize = True
    if False:
        validate_image_node_1.metadata[DL_NodeInputMetadata.graph_optimize] = False

    # Job Script Example A
    if True:
        validate_image_node_1.metadata[DL_NodeInputMetadata.job_script_type] = DL_JobScriptType.post
        validate_image_node_2.metadata[DL_NodeInputMetadata.job_script_type] = DL_JobScriptType.post

    # Job Interpreter
    # render_image_node_1.metadata[DL_NodeInputMetadata.interpreter] = "houdini"

    # Subgraph
    if True:
        # optimize = False
        stats_graph = Graph(name="Statistics")
        stats_collect_node = CollectStatisticsNode(
            graph=stats_graph, name="Collect Stats"
        )
        stats_send_node = SendStatisticsNode(graph=stats_graph, name="Send Stats")
        stats_collect_node.outputs["stats"] >> stats_send_node.inputs["stats"]
        stats_collect_node.inputs["dummy_input"].promote_to_graph(name="Input")
        stats_send_node.outputs["dummy_output"].promote_to_graph(name="Output")
        update_db_node.outputs["status"] >> stats_graph.inputs["Input"]

    # Submit
    print(graph)
    jobs = scheduler.dl_send_graph_to_farm(connection, graph, optimize=optimize)


if __name__ == "__main__":
    connection = Connect.DeadlineCon("localhost", 8081)
    dl_flowpipe_example_vfx_submit(connection)
