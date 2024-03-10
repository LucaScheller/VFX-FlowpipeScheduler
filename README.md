# FlowPipe Scheduler

This repository is still work in progress, please check back at a later time.

Schedulers:
- Deadline

## Features
This repository holds the following extended features to [flowpipe](https://github.com/PaulSchweizer/flowpipe):
- Deadline farm submission:
    - Automatic batching per node with automatic frame dependencies via common naming conventions on nodes:
        - Inputs: "batch_items", "batch_size"
        - Metadata: "batch_frame_offset" 
    - Job configuration injection using our Deadline API wrapper in [VFX-DeadlineRepository](https://github.com/LucaScheller/VFX-DeadlineRepository)

