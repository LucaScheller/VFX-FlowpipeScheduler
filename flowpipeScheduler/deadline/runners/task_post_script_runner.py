import os
import sys

# TODO Remove hardcoded boostrap
sys.path.insert(0, "/mnt/data/PROJECT/VFX-DeadlineRepository/library/python")

# Bootstrap
# TODO Remove this and put on PYTHONPATH
try:
    from flowpipeScheduler.scheduler import evaluate_on_farm_through_env
except ModuleNotFoundError:
    current_dir = os.path.dirname(__file__)
    module_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    sys.path.insert(0, module_dir)
    from flowpipeScheduler.scheduler import dl_job_script_evaluate_on_farm_through_env

if __name__ == "__main__":
    dl_job_script_evaluate_on_farm_through_env("task_post")