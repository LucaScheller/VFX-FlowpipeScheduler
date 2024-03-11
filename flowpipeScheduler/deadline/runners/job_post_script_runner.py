import os
import sys
# Deadline fails to import relative modules.
sys.path.insert(0, os.path.dirname(__file__))
import utils
dl_job_script_evaluate_on_farm_through_env = utils.bootstrap_dl_job_script_evaluate_on_farm_through_env()

def __main__(*args):
    deadlinePlugin = args[0]
    job = deadlinePlugin.GetJob()
    dl_job_script_evaluate_on_farm_through_env("job_post", batch_range=None)
