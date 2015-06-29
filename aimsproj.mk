tooldir = ~/projects/comp-chem-tools
tools = dispatcher.py geomlib.py pyaims.py worker.py
remotedir = ~/calculations
AIMSROOT ?= ~/builds/fhi-aims
aimsroot_remote = ~/software/fhi-aims

.PHONY: all prepare run_% update remote_% upload_% check monitor_% clean distclean

all: RUN/local_job.log

RUN/%_job.log: prepare.py ${tools} run_aims.sh ${prereq}
ifneq ("$(wildcard RUN/*.start RUN/*.running.*)", "")
	$(error "Some jobs are still running.")
endif
	make prepare
	make run_$*

${tools} aimsproj.mk: %:
	rsync -a ${tooldir}/$@ $@

run_aims.sh:
	rsync -a ~/bin/$@ $@

prepare:
	-rm -r RUN
	AIMSROOT=${AIMSROOT} python prepare.py

run_local:
	python worker.py RUN 1 >RUN/local_job.log

run_%:
	bash ~/bin/submit.sh $*.job.sh

update:
	make -B ${tools} aimsproj.mk run_aims.sh

remote_%:
	$(eval remote := $(firstword $(subst _, , $*)))
	make upload_${remote}
	ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && AIMSROOT=${aimsroot_remote} make RUN/$*_job.log"

submit_%:
	$(eval remote := $(firstword $(subst _, , $*)))
	ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && make run_$*"

upload_%:
	make ${tools} run_aims.sh
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

distclean: clean
	-rm -r RUN
	-rm ${tools}
	-rm run_aims.sh

distclean_%:
	ssh $* "cd ${remotedir}/$(notdir ${PWD}) && make distclean"
