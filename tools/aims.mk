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
BRANCH ?= master
updatehash := $(shell realpath ${AIMSROOT}/build/${BRANCH}/bin/aims.latest | tail -c 8)
updatetar = ${AIMSROOT}/diffs/${updatehash}.diff.tar.gz

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
	@echo "Fetching aims from branch ${BRANCH} with SHA hash ${updatehash}..."
	@rsync -a ${updatetar} aims.tar.gz
