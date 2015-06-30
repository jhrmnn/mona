tools = dispatcher.py geomlib.py pyaims.py worker.py
remotedir = ~/calculations
AIMSROOT ?= ~/builds/fhi-aims
aimsroot_remote = ~/software/fhi-aims

.PHONY: all prepare run_% update remote_% upload_% check monitor_% clean distclean
.SECONDEXPANSION:

all: RUN/local_job.log

RUN/%_job.log: prepare.py external ${prereq}
ifneq ("$(wildcard RUN/*.start RUN/*.running.*)", "")
	$(error "Some jobs are still running.")
endif
	make prepare
	make run_$*

${tools} aimsproj.mk:
	rsync -a ${tooldir}/$@ $@

run_aims.sh:
	rsync -a ~/bin/$@ $@

prepare:
ifneq ("$(wildcard RUN)", "")
	$(error "There is a previous RUN, run make cleanrun to overwrite.")
endif
	AIMSROOT=${AIMSROOT} python prepare.py

run_local:
	python worker.py RUN 1 >RUN/local_job.log

run_%:
	bash ~/bin/submit.sh $*.job.sh
	@sleep 0.3  # some submitters print asynchronously

update:
	make -B external

external: ${tools} run_aims.sh aimsproj.mk

remote_%: upload_$$(firstword $$(subst _, , %))
	$(eval remote := $(firstword $(subst _, , $*)))
	ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && AIMSROOT=${aimsroot_remote} make RUN/$*_job.log"

submit_%:
	$(eval remote := $(firstword $(subst _, , $*)))
	ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && make run_$*"

upload_%: external
	rsync -a --delete \
		--exclude=*.pyc --exclude=RUN --exclude=run_aims.sh \
		--include=$*_*.job.sh --exclude=*_*.job.sh \
		${PWD} $*:${remotedir}/

check:
	grep "Have a nice day." RUN/*.done/rundir/run.log

monitor_%:
	ssh $* qmy

clean:
	-rm *.pyc

cleanrun:
	-rm -r RUN

distclean: clean cleanrun
	-rm ${tools}
	-rm run_aims.sh

cleanrun_%:
	ssh $* "cd ${remotedir}/$(notdir ${PWD}) && make cleanrun"

distclean_%:
	ssh $* "cd ${remotedir}/$(notdir ${PWD}) && make distclean"
