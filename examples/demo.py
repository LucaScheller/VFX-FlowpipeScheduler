import json
import os
import subprocess
import sys
import uuid

import DeadlineWebService.DeadlineConnect as Connect
from deadlineAPI.Deadline.Jobs import (DateTime, FrameList, Job,
                                       JobScheduledType, JobStatus, TimeSpan)
from deadlineConfigure.etc.constants import (EnvironmentVariables,
                                             JobDependencyScripts)
from flowpipe import Graph
from flowpipeScheduler import scheduler
from flowpipeScheduler.scheduler import DL_JobScriptType, DL_NodeInputMetadata
from nodes import *

def api_frame_list():
    pass
    """
    l = list(range(0,20, 5))
    l = reversed(l)
    l = [0,5,10, 15, 11,12,13, 14, 15, 27, 31,109]
    frameStr = FrameList.convertFrameListToFrameString(l)
    frameList = FrameList.convertFrameStringToFrameList(frameStr)
    print("Frame List", list(l) == frameList, frameStr, frameList, list(l))
    """


def api_job_query(connection):
    job_ids = connection.Jobs.GetJobIds()
    # print(job_ids)
    job_data = connection.Jobs.GetJob(job_ids[0])
    print(json.dumps(job_data, indent=4))

    return
    job_data = api_job_query(connection)
    job = Job().deserializeWebAPI(job_data)
    print(json.dumps(job_data, indent=4))


    job = Job()
    job.JobName = "Some Name"
    job.JobBatchName = "Some Cool Batch"
    print(job.JobMinRenderTimeSeconds)
    job.JobMinRenderTimeSeconds = 360
    job.JobMachineLimit = 100
    job.JobPostJobScript = "/home/lucsch/Downloads/ahaaaaaaaaaaaaaaa.py"
    job.SetJobLimitGroups(["cache"])
    print(job.JobMinRenderTimeSeconds)
    job_data = job.serializeWebAPI()
    print(job_data)
    api_job_update(connection, job_data)


    return job_data


def api_job_update(connection, job_data):
    state = connection.Jobs.SaveJob(job_data)
    print(state)
    # print(json.dumps(job_data, indent=4))


def api_job_submit(connection):
    submission_directory_path = os.path.join(os.path.dirname(__file__), "submission")
    aux_submission_file_path = os.path.join(submission_directory_path, "aux.txt")

    job = Job()
    job.JobName = "Test Submit"
    job.JobPlugin = "Command"
    job.JobStatus = JobStatus.Suspended
    job.JobAuxiliarySubmissionFileNames = [aux_submission_file_path]
    job.JobOutputDirectories = ["/home/lucsch/Downloads"]
    job.JobMachineLimit = 66
    job.JobFramesList = range(1001, 1010) #list(range(0, 101, 25)) + list(range(0, 101))
    job.JobFramesPerTask = 3
    job.JobListedSlaves = ["station1-hydra-home"]
    job.JobFailureDetectionJobErrors = 79
    job.JobFailureDetectionTaskErrors = 69
    job.JobMinRenderTimeSeconds = 120
    job.JobPostJobScript = "/home/lucsch/Downloads/hdGp.png"
    job.JobScheduledType = JobScheduledType.Daily
    job.JobScheduledStartDateTime = DateTime(year=2025, month=1, day=5)
    job.JobScheduledStopDateTime = DateTime(year=2026, month=1, day=5)
    job.JobScheduledDays = 3
    job.JobMondayStartTime = TimeSpan(hour=18)
    job.JobMondayStopTime = TimeSpan(hour=20)

    
    jobData, pluginData, auxFilePaths = job.serializeSubmissionCommandlineDictionaries()
    connection.Jobs.SubmitJob(jobData, pluginData, auxFilePaths)

    """
    submission_directory_path = os.path.join(os.path.dirname(__file__), "submission")
    job_submission_file_path = os.path.join(submission_directory_path, "job.txt")
    plugin_submission_file_path = os.path.join(submission_directory_path, "plugin.txt")
    aux_submission_file_path = os.path.join(submission_directory_path, "aux.txt")
    job_submission_args = job.serializeSubmissionCommandlineFiles(job_submission_file_path,
                                                                plugin_submission_file_path)
    

    deadline_directory_path = "/opt/Thinkbox/DeadlineClient10/bin"
    job_submission_args.insert(0, "deadlinecommand")
    print("Submission Command", job_submission_args)
    subprocess.check_call(job_submission_args, stdout=sys.stdout, stderr=sys.stderr, shell=False, env={"PATH": "{}{}{}".format(deadline_directory_path, 
                                                                                                                               os.pathsep,
                                                                                                                               os.environ["PATH"])})
    """


def api_job_submit_python(connection):
    # Python Job
    jobInfo = {
        "Name": "Submitted via Python",
        "UserName": "UserName",
        "Frames": "1001-1010",
        "Plugin": "Python",
    }
    pluginInfo = {
        "ScriptFile": "/mnt/data/PROJECT/VFX-DeadlineFlowpipe/reference/task.py",
        "Version": "3.10",
        "Arguments": "-arg A -arg B",
    }
    try:
        jobDetails = connection.Jobs.SubmitJob(jobInfo, pluginInfo)
        print(json.dumps(jobDetails, indent=4))
    except:
        print("Sorry, Web Service is currently down!")


def api_job_submit_husk(connection):
    # Husk Job
    jobInfo = {
        "Name": "Example - Husk",
        "UserName": "UserName",
        "Frames": "1001-1010",
        "ChunkSize": "5",
        "Plugin": "HoudiniHusk",
        "EnvironmentKeyValue0": "EXAMPLE_ENV_KEY=EXAMPLE_ENV_VALUE",
        "ConcurrentTasks": "1",
    }
    pluginInfo = {
        "HoudiniVersion": "20.0",
        "Arguments": "--usd-input /mnt/data/PROJECT/VFX-FlowpipeScheduler/reference/scene.usda --frame <FRAME> --frame-count <FRAME_COUNT> --frame-inc <FRAME_INCREMENT> --fast-exit 1 --verbose acet3",
    }
    try:
        jobDetails = connection.Jobs.SubmitJob(jobInfo, pluginInfo)
        print(json.dumps(jobDetails, indent=4))
    except:
        print("Sorry, Web Service is currently down!")


def api_job_submit_command(connection):
    # Command Job
    jobInfo = {
        "Name": "Example - Command",
        "UserName": "lucsch",
        "Frames": "1001-1010",
        "ChunkSize": "1",
        "Plugin": "Command",
        "EnvironmentKeyValue0": "EXAMPLE_ENV_KEY=EXAMPLE_ENV_VALUE",
        "EnvironmentKeyValue1": "{}=1".format(
            EnvironmentVariables.JOB_PENDING_REVIEW_RELEASE_STATE
        ),
        "EnvironmentKeyValue2": "{}=5".format(
            EnvironmentVariables.JOB_PENDING_REVIEW_RELEASE_INCREMENT
        ),
        "ExtraInfo0": "True",
        "ConcurrentTasks": "1",
        "IsFrameDependent": "1",
        "ScriptDependencies": JobDependencyScripts.pending_review_release,
    }
    pluginInfo = {
        "Command": "python /mnt/data/PROJECT/VFX-FlowpipeScheduler/reference/debug.py <FRAME_START> <FRAME_END> <FRAME_INCREMENT>"
    }
    try:
        jobDetails = connection.Jobs.SubmitJob(jobInfo, pluginInfo)
        print(json.dumps(jobDetails, indent=4))
    except:
        print("Sorry, Web Service is currently down!")


def api_job_submit_dependency(connection):
    submission_directory_path = os.path.join(os.path.dirname(__file__), "submission")
    aux_submission_file_path = os.path.join(submission_directory_path, "aux.txt")

    submission_batch_hash = uuid.uuid4().hex

    # Job A
    job_0 = Job()
    job_0.JobName = "Job 0"
    job_0.JobBatchName = "Job Dependency Test ({})".format(submission_batch_hash)
    job_0.JobStatus = JobStatus.Active
    job_0.JobPriority = 50
    job_0.JobOutputDirectories = ["/home/lucsch/Downloads"]
    job_0.JobFramesList = range(1001, 1010) #list(range(0, 101, 25)) + list(range(0, 101))
    job_0.JobFramesPerTask = 1
    job_0.JobPlugin = "Command"
    job_0.SetJobPluginInfoKeyValue("Command", "python /mnt/data/PROJECT/VFX-FlowpipeScheduler/reference/task.py")
    #job.JobPostJobScript = "/home/lucsch/Downloads/hdGp.png"

    jobData, pluginData, auxFilePaths = job_0.serializeSubmissionCommandlineDictionaries()
    jobWebServiceData = connection.Jobs.SubmitJob(jobData, pluginData, auxFilePaths)
    if not jobWebServiceData:
        raise Exception("Failed to submit job.")
    job_0.deserializeWebAPI(jobWebServiceData)
    
    # Job B
    job_1 = Job()
    job_1.JobName = "Job 1"
    job_1.JobBatchName = "Job Dependency Test ({})".format(submission_batch_hash)
    job_1.JobStatus = JobStatus.Active
    job_1.JobPriority = 100
    job_1.JobOutputDirectories = ["/home/lucsch/Desktop"]
    job_1.JobFramesList = range(1001, 1010) #list(range(0, 101, 25)) + list(range(0, 101))
    job_1.JobFramesPerTask = 1
    job_1.JobPlugin = "Command"
    job_1.SetJobPluginInfoKeyValue("Command", "python /mnt/data/PROJECT/VFX-FlowpipeScheduler/reference/task.py")
    job_1.SetJobDependencyIDs([job_0.JobId])
    #job.JobPostJobScript = "/home/lucsch/Downloads/hdGp.png"

    jobData, pluginData, auxFilePaths = job_1.serializeSubmissionCommandlineDictionaries()
    jobWebServiceData = connection.Jobs.SubmitJob(jobData, pluginData, auxFilePaths)
    if not jobWebServiceData:
        raise Exception("Failed to submit job.")
    job_1.deserializeWebAPI(jobWebServiceData)


def api_fl_dl_submit(connection):
    graph = Graph(name="Rendering")

    db_node = UpdateDatabase(graph=graph, id_=123456, name="Update Database")

    renderImageA = RenderImage(
        name="SomeRender A",
        graph=graph
    )
    renderImageA.outputs["value"]["0"].connect(
        db_node.inputs["value"][str(0)]
    )

    job_overrides = Job()
    job_overrides.JobPriority = 200
    batch_items = list(range(1001, 1020))
    renderImageC = RenderImage(
        name="SomeRender C",
        batch_items=batch_items,
        batch_size=5,
        metadata={DL_NodeInputMetadata.batch_frame_offset: 1001,
                  DL_NodeInputMetadata.interpreter: "python",
                  DL_NodeInputMetadata.job_script_type: DL_JobScriptType.post,
                  DL_NodeInputMetadata.job_overrides: job_overrides},
        graph=graph
    )
    renderImageC.outputs["value"]["1"].connect(
        db_node.inputs["value"][str(1)]
    )
    
    #batch_items = list(range(1, 20))
    renderImageB = RenderImage(
        name="SomeRender B",
        batch_items=batch_items,
        batch_size=5,
        metadata={DL_NodeInputMetadata.batch_frame_offset: 1001},
        graph=graph
    )
    renderImageB.outputs["value"]["1"].connect(
        renderImageC.inputs["value"]
    )

    renderSceneDescr = RenderSceneDescripition(
        name="Scene Descr",
        graph=graph
    )
    renderSceneDescr.outputs["value"].connect(
        renderImageA.inputs["value"]
    )
    renderSceneDescr.outputs["value"].connect(
        renderImageB.inputs["value"]
    )

    print(graph)

    scheduler.dl_send_graph_to_farm(connection, graph)


if __name__ == "__main__":
    connection = Connect.DeadlineCon("localhost", 8081)
    #api_fl_dl_submit(connection)
    api_fl_dl_submit(connection)
