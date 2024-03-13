# Repo Setup
> export REPO_ROOT=$PROJECT/VFX-FlowpipeScheduler
## Sub-Modules
> git submodule add https://github.com/LucaScheller/flowpipe.git ext/flowpipe
> git submodule add https://github.com/LucaScheller/NodeGraphQt.git ext/NodeGraphQt
## Python
> python3.12 -m venv $PROJECT/VFX-FlowpipeScheduler/ext/python
Manually edit shell line prefix to "VFX-DeadlineFlowpipe-Python" in:
$PROJECT/VFX-FlowpipeScheduler/ext/python/bin/activate
### Dependency Links
> ln -s -r ext/flowpipe/flowpipe ext/python/lib/python3.12/site-packages/flowpipe
> ln -s -r ext/NodeGraphQt/NodeGraphQt ext/python/lib/python3.12/site-packages/NodeGraphQt
### Pip Installs
> source $REPO_ROOT/ext/python/bin/activate
> pip install ascii_canvas
> pip install /opt/Thinkbox/DeadlineRepository10/api/scripting/deadline-stubs-10.3.1.4.tar.gz

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
