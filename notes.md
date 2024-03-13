# Repo Setup
> export REPO_ROOT=$PROJECT/VFX-FlowpipeScheduler
## Sub-Modules
> git submodule add https://github.com/LucaScheller/flowpipe.git external/flowpipe
> git submodule add https://github.com/LucaScheller/NodeGraphQt.git external/NodeGraphQt
## Python
> python3.12 -m venv $PROJECT/VFX-FlowpipeScheduler/external/python
Manually edit shell line prefix to "VFX-DeadlineFlowpipe-Python" in:
$PROJECT/VFX-FlowpipeScheduler/external/python/bin/activate
### Dependency Links
> ln -s -r external/flowpipe/flowpipe external/python/lib/python3.12/site-packages/flowpipe
> ln -s -r external/NodeGraphQt/NodeGraphQt external/python/lib/python3.12/site-packages/NodeGraphQt
### Pip Installs
> source $REPO_ROOT/external/python/bin/activate
> pip install ascii_canvas
> pip install /opt/Thinkbox/DeadlineRepository10/api/scripting/deadline-stubs-10.3.1.4.tar.gz
> pip install PySide2

# Repo Source
## Dev Env
source setup.sh
## Start
redis-server
deadline-start
deadline-webservice
### Inspection
redis-insight
### Workers
$DEADLINE_INSTALL_ROOT/DeadlineClient10/bin/deadlineworker
$DEADLINE_INSTALL_ROOT/DeadlineClient10/bin/deadlineworker -name "background"
## Stop
deadline-stop
redis-cli shutdown


# ToDos
- Add partial redis json file editing via the json editing capabilities of redis
- Add further graph optimization:
    - Add deadline support for putting all nodes into a single job (if the interpreter allows it)
