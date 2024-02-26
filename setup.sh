# Source repo
if [ ! $REPO_SOURCED ]
then
    # Define repo root
    export REPO_SOURCED=1
    export REPO_ROOT=$PROJECT/VFX-FlowpipeDeadline
    # Source Python
    source $REPO_ROOT/ext/python/bin/activate
    export PYTHONDONTWRITEBYTECODE=1 # Disable __pycache__ byte code generation
    # Add external Python packages
    export DEADLINE_INSTALL_ROOT=/opt/Thinkbox
    export PYTHONPATH=$REPO_ROOT/src:$DEADLINE_INSTALL_ROOT/DeadlineRepository10/api/python:$DEADLINE_INSTALL_ROOT/DeadlineRepository10/custom/library/python:$PYTHONPATH
fi