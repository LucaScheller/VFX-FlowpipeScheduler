from flowpipe import Graph, Node


@Node(outputs=["value"], metadata={"interpreter": "python"})
def RenderSceneDescripition(value):
    """Render the given frames from the given scene.."""
    print("HEEEEEEEEEE Running Render Scene Descr")
    return {"value": 5}

@Node(outputs=["value"], metadata={"interpreter": "python"})
def RenderImage(value, batch_items, batch_size):
    """Render the given frames from the given scene.."""
    print("HEEEEEEEEEE Running Render Image", batch_items)
    return {"value": value + 10, "value.0": 10, "value.1": 10}


@Node(outputs=["status"], metadata={"interpreter": "python"})
def UpdateDatabase(id_, value):
    """Update the database entries of the given asset with the given data."""
    print("HEEEEEEEEEEEEEEE", value)
    return {"status": "nothing"}
    value = sum(value.values())
    

    return {"status": value}
