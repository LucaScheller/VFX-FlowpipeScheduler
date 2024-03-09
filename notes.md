# Repo Setup
> export REPO_ROOT=$PROJECT/VFX-DeadlineFlowpipe
## Sub-Modules
> git submodule add https://github.com/LucaScheller/flowpipe.git ext/flowpipe
## Python
> python3.12 -m venv /mnt/data/PROJECT/VFX-DeadlineFlowpipe/ext/python

Manually edit shell line prefix to "VFX-DeadlineFlowpipe-Python" in:
/mnt/data/PROJECT/VFX-DeadlineFlowpipe/ext/python/bin/activate
### Dependency Links
> ln -s -r ext/flowpipe/flowpipe ext/python/lib/python3.12/site-packages/flowpipe
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
- Add deadline minimal support
- Add Reddis Backend
- Add Multi-Interpreters
- Add deadline scripts support
- Add deadline custom dependency support/task grouping for node interpreters/task dependency instead of job dependency