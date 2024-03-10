import os
import sys


def boostrap_common():
    # TODO Remove hardcoded boostrap
    sys.path.insert(0, "/mnt/data/PROJECT/VFX-DeadlineRepository/library/python")
    sys.path.insert(
        1,
        "/mnt/data/PROJECT/VFX-FlowpipeScheduler/ext/python/lib/python3.12/site-packages",
    )


def bootstrap_evaluate_on_farm_through_env():
    # Bootstrap common
    boostrap_common()
    # Bootstrap runner
    # TODO Remove this and put on PYTHONPATH
    try:
        from flowpipeScheduler.scheduler import evaluate_on_farm_through_env
    except ModuleNotFoundError:
        current_dir = os.path.dirname(__file__)
        module_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        sys.path.insert(0, module_dir)
        from flowpipeScheduler.scheduler import evaluate_on_farm_through_env

    return evaluate_on_farm_through_env


def bootstrap_dl_job_script_evaluate_on_farm_through_env():
    # Bootstrap common
    boostrap_common()
    # Bootstrap runner
    # TODO Remove this and put on PYTHONPATH
    try:
        from flowpipeScheduler.scheduler import (
            dl_job_script_evaluate_on_farm_through_env,
        )
    except ModuleNotFoundError:
        current_dir = os.path.dirname(__file__)
        module_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        sys.path.insert(0, module_dir)
        from flowpipeScheduler.scheduler import (
            dl_job_script_evaluate_on_farm_through_env,
        )

    return dl_job_script_evaluate_on_farm_through_env
