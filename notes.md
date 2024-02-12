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