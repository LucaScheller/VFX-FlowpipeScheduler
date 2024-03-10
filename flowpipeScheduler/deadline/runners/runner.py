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
    from flowpipeScheduler.scheduler import evaluate_on_farm_through_env

if __name__ == "__main__":
    frame_start, frame_end = sys.argv[1:3]
    frame_start = int(frame_start)
    frame_end = int(frame_end)
    evaluate_on_farm_through_env(batch_range=(frame_start, frame_end, 1))