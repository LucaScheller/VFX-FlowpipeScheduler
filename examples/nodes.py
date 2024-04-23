import os

from flowpipe import Node

# -----------------------------------------------------------------------------
#
# VFX Pipeline
#
# -----------------------------------------------------------------------------


@Node(outputs=["sceneDescription_file_path"], metadata={"interpreter": "python"})
def GenerateSceneDescriptionNode(scene_file_path):

    print("Running generate scene description.")
    return {"sceneDescription_file_path": "/some/file/path/render.usd"}


@Node(outputs=["image_file_path"], metadata={"interpreter": "python"})
def RenderImageNode(sceneDescription_file_path, renderpass, batch_items, batch_size):
    print("Running render image on {} items".format(len(batch_items)))
    image_file_name = renderpass + ".{:04d}.exr"
    image_file_path = os.path.join("/some/file/path", image_file_name)
    for item in batch_items:
        print("--> Running render image on batch item",
              sceneDescription_file_path, renderpass, image_file_path.format(item))
    return {"image_file_path": image_file_path}


@Node(outputs=["image_file_path"], metadata={"interpreter": "python"})
def ValidateImageNode(image_file_path, batch_items, batch_size):
    print("Running validate image on {} items".format(len(batch_items)))
    for item in batch_items:
        print("--> Running validate image on batch item", image_file_path.format(item))
    return {"image_file_path": image_file_path}


@Node(outputs=["image_file_path"], metadata={"interpreter": "python"})
def NotifyUserNode(image_file_path, user_names):
    print("Notifying users", user_names , "about", image_file_path)
    return {"image_file_path": image_file_path}


@Node(outputs=["image_file_path"], metadata={"interpreter": "python"})
def MultipartConvertImageNode(image_file_path, batch_items, batch_size):
    print("Running multipart convert image on {} items".format(len(batch_items)))
    for item in batch_items:
        print("--> Running multipart convert image on batch item", image_file_path.format(item))
    return {"image_file_path": image_file_path}


@Node(outputs=["image_file_path", "denoise_image_file_path"], metadata={"interpreter": "python"})
def DenoiseImageNode(image_file_path, batch_items, batch_size):
    print("Running denoise image on {} items".format(len(batch_items)))
    for item in batch_items:
        print("--> Running denoise image on batch item", image_file_path.format(item))
    return {"image_file_path": image_file_path,
            "denoise_image_file_path": "/some/file/path/denoise_image.{:04d}.exr"}


@Node(outputs=["status"], metadata={"interpreter": "python"})
def UpdateDatabaseNode(image_file_paths):
    print("Running update database on images", image_file_paths)
    status = "Successful"
    return {"status": status}


@Node(outputs=["stats"], metadata={"interpreter": "python"})
def CollectStatisticsNode(dummy_input):
    print("Running collect statistics")
    return {"stats": dummy_input}


@Node(outputs=["stats"], metadata={"interpreter": "python"})
def ProcessStatisticsNode(stats):
    print("Running process statistics")
    return {"stats": stats}


@Node(outputs=["dummy_output"], metadata={"interpreter": "python"})
def SendStatisticsNode(stats):
    print("Running send statistics", stats)
    return {"dummy_output": None}