ifndef inputs
$(error "Project has no defined $${inputs}.")
endif
ifndef outputs
$(error "Project has no defined $${outputs}.")
endif
ifndef tooldir
$(error "The $${tooldir} path is not defined.")
endif
tools = dispatcher.py geomlib.py pyaims.py worker.py logparser.py
external = ${tools} aimsproj.mk run_aims.sh
remotedir = ~/calculations
AIMSROOT ?= ~/builds/fhi-aims
aimsroot_remote = ~/software/fhi-aims

.SECONDEXPANSION:
.PRECIOUS: $(addprefix results_%/, ${outputs}) results_%/results.p RUN/%_job.log

all:
	@make --no-print-directory $(addprefix results_local/, ${outputs})

$(addprefix results_%/, ${outputs}): results_%/results.p process.py
	cd results_$* && python ../process.py ../$<

results_%/results.p: RUN/%_job.log extract.py | ${external}
	python extract.py
	mkdir -p results_$* && mv RUN/results.p $@

RUN/%_job.log: prepare.py ${inputs} | ${external}
ifneq ("$(wildcard RUN/*.start RUN/*.running.*)", "")
	$(error "Some jobs are still running.")
endif
	@make --no-print-directory prepare
	@make --no-print-directory run_$*

${tools} aimsproj.mk:
	@rsync -ai ${tooldir}/$@ $@

run_aims.sh:
	@rsync -ai ~/bin/$@ $@

run_local:
	python worker.py RUN 1 >RUN/local_job.log

run_%:
	bash ~/bin/submit.sh $*.job.sh
	@sleep 1  # some submitters print asynchronously
	@make --no-print-directory print_error
	
prepare:
ifneq ("$(wildcard RUN)", "")
	$(error "There is a previous RUN, run make cleanrun to overwrite.")
endif
	AIMSROOT=${AIMSROOT} python prepare.py

print_error:
	$(error "Wait till the job finishes, then run make again.")

update:
	@echo "Updating tools..."
	@make --no-print-directory -B external

external: ${external}

remote_%: upload_$$(firstword $$(subst _, , %)) 
	$(eval remote := $(firstword $(subst _, , $*)))
ifndef OFFLINE
	@echo "Connecting to ${remote}..."
	@ssh ${remote} \
		"cd ${remotedir}/$(notdir ${PWD}) && \
		AIMSROOT=${aimsroot_remote} make results_$*/results.p"
	@echo "Downloading results from ${remote}..."
	@rsync -ia ${remote}:${remotedir}/$(notdir ${PWD})/results_$*/results.p results_$*/
endif
	@make --no-print-directory $(addprefix results_$*/, ${outputs})

upload_%: ${external}
ifdef OFFLINE
	@echo "Skipping upload."
else
	@echo "Uploading to $*..."
	@rsync -ia --delete \
		--exclude=*.pyc --exclude=RUN --exclude=run_aims.sh \
		--include=$*_*.job.sh --exclude=*_*.job.sh \
		--exclude=results_* \
		${PWD}/* $*:${remotedir}/$(notdir ${PWD})/
endif

submit_%:
	$(eval remote := $(firstword $(subst _, , $*)))
	@echo "Connecting to ${remote}..."
	@ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && make run_$*"

check:
	grep "Have a nice day." RUN/*.done/rundir/run.log

monitor_%:
	@ssh $* qmy

clean:
ifneq ("$(wildcard *.pyc)", "")
	rm *.pyc
endif

cleanrun:
ifneq ("$(wildcard RUN)", "")
	rm -r RUN
endif

distclean: clean cleanrun
ifneq ("$(wildcard ${tools} run_aims.sh results_*/*)", "")
	rm $(wildcard ${tools} run_aims.sh results_*/*)
endif
ifneq ("$(wildcard results_*)", "")
	rmdir results_*
endif

cleanrun_%:
	@echo "Connecting to $*..."
	@ssh $* "cd ${remotedir}/$(notdir ${PWD}) && make cleanrun"

distclean_%:
	@echo "Connecting to $*..."
	@ssh $* "cd ${remotedir}/$(notdir ${PWD}) && make distclean"
