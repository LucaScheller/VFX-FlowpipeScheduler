import os
import sys
# Deadline fails to import relative modules.
sys.path.insert(0, os.path.dirname(__file__))
import utils
dl_job_script_evaluate_on_farm_through_env = utils.bootstrap_dl_job_script_evaluate_on_farm_through_env()

def __main__(*args):
    deadlinePlugin = args[0]
    job = deadlinePlugin.GetJob()
    task = deadlinePlugin.GetCurrentTask()
    batch_range = [f for f in task.TaskFrameList]
    batch_range = [batch_range[0], batch_range[-1], 1]
    dl_job_script_evaluate_on_farm_through_env("job_task_pre", batch_range=batch_range)
