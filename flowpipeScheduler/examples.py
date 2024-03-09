import json
import logging
import os
import sys
from tempfile import gettempdir

import redis
from flowpipe import Graph, INode, Node


# -----------------------------------------------------------------------------
#
# Examples
#
# -----------------------------------------------------------------------------


@Node(outputs=["renderings"], metadata={"interpreter": "maya"})
def MayaRender(batch_items, scene_file):
    """Render the given frames from the given scene.."""
    return {"renderings": "/renderings/file.%04d.exr"}


@Node(outputs=["status"])
def UpdateDatabase(id_, images):
    """Update the database entries of the given asset with the given data."""
    return {"status": True}


def implicit_batching(frames, batch_size):
    """Batches are created during the farm conversion."""
    graph = Graph(name="Rendering")
    render = MayaRender(
        graph=graph,
        batch_items=list(range(frames)),
        scene_file="/scene/for/rendering.ma",
        metadata={"batch_size": batch_size},
    )
    update = UpdateDatabase(graph=graph, id_=123456)
    render.outputs["renderings"].connect(update.inputs["images"])

    print(graph)
    print(json.dumps(convert_graph_to_job(graph), indent=2))


def explicit_batching(frames, batch_size):
    """Batches are already part of the graph."""
    graph = Graph(name="Rendering")
    update_database = UpdateDatabase(graph=graph, id_=123456)
    for i in range(0, frames, batch_size):
        maya_render = MayaRender(
            name="MayaRender{0}-{1}".format(i, i + batch_size),
            graph=graph,
            frames=list(range(i, i + batch_size)),
            scene_file="/scene/for/rendering.ma",
        )
        maya_render.outputs["renderings"].connect(
            update_database.inputs["images"][str(i)]
        )

    print(graph)
    print(json.dumps(convert_graph_to_job(graph), indent=2))


if __name__ == "__main__":
    implicit_batching(30, 10)
    explicit_batching(30, 10)
