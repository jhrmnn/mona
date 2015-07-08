tools += geomlib.py pyaims.py logparser.py
external += projaims.mk run_aims.sh
excluded += run_aims.sh
remotedir ?= ~/calculations
ifdef REMOTE
AIMSROOT = ${HOME}/software/fhi-aims
else
AIMSROOT = ${HOME}/projects/fhi-aims
endif
export AIMSROOT

include proj.mk

projaims.mk:
	@rsync -ai ${tooldir}/$@ $@

run_aims.sh:
	@rsync -ai ~/bin/$@ $@
	
check:
	@echo "Number of successfull tasks: $(shell \
		grep "Have a nice day." RUN/*.done/rundir/run.log 2>/dev/null | wc -l)"

tellaims:
	@tar -xO <aims.tar.gz diff | shasum | awk '{print $$1}' | tail -c8

updateaims:
	@rsync -a `realpath ${AIMSROOT}/diffs/latest.diff.tar.gz` aims.tar.gz
