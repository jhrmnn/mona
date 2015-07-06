tools = geomlib.py pyaims.py logparser.py
external = projaims.mk run_aims.sh
remotedir = ~/calculations
AIMSROOT ?= ~/projects/fhi-aims
aimsroot_remote = ~/software/fhi-aims
excluded = run_aims.sh
prepare_env = AIMSROOT=${AIMSROOT}
prepare_env_remote = AIMSROOT=${aimsroot_remote}

include proj.mk

projaims.mk:
	@rsync -ai ${tooldir}/$@ $@

run_aims.sh:
	@rsync -ai ~/bin/$@ $@
	
check:
	grep "Have a nice day." RUN/*.done/rundir/run.log

