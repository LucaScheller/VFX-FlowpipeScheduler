import sys

import utils
evaluate_on_farm_through_env = utils.bootstrap_evaluate_on_farm_through_env()

if __name__ == "__main__":
    frame_start, frame_end = sys.argv[1:3]
    frame_start = int(frame_start)
    frame_end = int(frame_end)
    evaluate_on_farm_through_env(batch_range=(frame_start, frame_end, 1))