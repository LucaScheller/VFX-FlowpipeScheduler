# Flowpipe Scheduler

Supported schedulers:
- Deadline

## Features
This repository holds the following extended features to [flowpipe](https://github.com/PaulSchweizer/flowpipe):
### Farm Submission
Features:
- Automatic batching per node with automatic frame dependencies via common naming conventions on nodes:
    - Inputs: "batch_items", "batch_size": The items to batch over with the given chunk size.
    - Metadata: "batch_frame_offset": The start frame of the tasks.
- Automatic database cleanup for:
    - Redis

Here is a full list of features that can be attached to any node:

- General:
    - Node Metadata:
        - "interpreter": The interpreter to use (e.g. "python", "houdini")
        - "interpreter": The interpreter version to use (e.g. "default", "3.9")
        - "batch_frame_offset": Visually offset the batch range instead of starting at 0
        - "graph_optimize": Collapse nodes with only a single child/parent into one based on dependency requirements. This defines weather to opt-out, when during farm conversion general graph conversion optimization is enabled.
    - Node Input Names:
        - "batch_items": A list of items to batch over (e.g. [1,2,3], [('charA', False), ('charB', True)])
        - "batch_size": The chunk size.

- Deadline:
    - Node Metadata:
        - "job_overrides": We can pass in a job object as defined in our [VFX-DeadlineRepository](https://github.com/LucaScheller/VFX-DeadlineRepository) to override any job settings (e.g. priority, limits, etc.)
        - "job_script_type": Opt-in to attaching the node as a pre/post job/task script if the requirements are met. Depending if the node is making use of batching (via "batch_items"), it will map it as a job task pre/post script or a job pre/post script.

> Currently the "batch_items" and "batch_size" are inputs that need to be defined before submission. We could also write them into our metadata, but to keep our implementation open for future improvements (e.g. dynamic batch size generation on farm), we choose to map them as "static/pre-submission" defined inputs for now.
 