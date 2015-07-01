ifndef inputs
$(error "Project has no defined $${inputs}.")
endif
ifndef outputs
$(error "Project has no defined $${outputs}.")
endif
ifndef tooldir
$(error "The $${tooldir} path is not defined.")
endif
tools += dispatcher.py worker.py
external += ${tools} proj.mk

.SECONDEXPANSION:
.PRECIOUS: $(addprefix results_%/,${outputs}) results_%/results.p RUN/%_job.log

all:
	@${MAKE} --no-print-directory $(addprefix results_local/,${outputs})

$(addprefix results_%/,${outputs}): results_%/results.p process.py
	cd results_$* && python ../process.py ../$<

results_%/results.p: RUN/%_job.log extract.py | ${external}
ifneq ("$(wildcard RUN/*.start RUN/*.running.*)", "")
	$(error "Some jobs are still running.")
endif
	python extract.py
	mkdir -p results_$* && mv RUN/results.p $@

RUN/%_job.log: prepare.py ${inputs} | ${external}
	@${MAKE} --no-print-directory prepare
	@${MAKE} --no-print-directory run_$*
	@$(if $(subst local,,$*), @${MAKE} --no-print-directory print_error)

print_error:
	$(error "Wait till the job finishes, then run make again.")

${tools} proj.mk:
	@rsync -ai ${tooldir}/$@ $@

run_local:
	python worker.py RUN 1 >RUN/local_job.log

run_%:
	bash ~/bin/submit.sh $*.job.sh
	@sleep 1  # some submitters print asynchronously
	
prepare:
ifneq ("$(wildcard RUN)", "")
	$(error "There is a previous RUN, run make cleanrun to overwrite.")
endif
	${prepare_env} python prepare.py

update:
	@echo "Updating tools..."
	@${MAKE} --no-print-directory -B external

external: ${external}

remote_%: upload_$$(firstword $$(subst _, , %)) 
ifdef OFFLINE
	@echo "Skipping download."
else
	$(eval remote := $(firstword $(subst _, ,$*)))
	@echo "Connecting to ${remote}..."
	@ssh ${remote} \
		"cd ${remotedir}/$(notdir ${PWD}) && \
		${prepare_env_remote} make results_$*/results.p"
	@echo "Downloading results from ${remote}..."
	@rsync -ia ${remote}:${remotedir}/$(notdir ${PWD})/results_$*/results.p results_$*/
endif
	@${MAKE} --no-print-directory $(addprefix results_$*/,${outputs})

upload_%: ${external}
ifdef OFFLINE
	@echo "Skipping upload."
else
	@echo "Uploading to $*..."
	@rsync -ia --delete \
		--exclude=*.pyc --exclude=RUN $(addprefix --exclude=,${excluded}) \
		--include=$*_*.job.sh --exclude=*_*.job.sh \
		--exclude=results_* \
		${PWD}/* $*:${remotedir}/$(notdir ${PWD})/
endif

submit_%:
	$(eval remote := $(firstword $(subst _, ,$*)))
	@echo "Connecting to ${remote}..."
	@ssh ${remote} "cd ${remotedir}/$(notdir ${PWD}) && make run_$*"

clean:
ifneq ("$(wildcard *.pyc)", "")
	rm *.pyc
endif

cleanrun:
ifneq ("$(wildcard RUN)", "")
	rm -r RUN
endif

distclean: clean cleanrun
ifneq ("$(wildcard ${tools} ${excluded} results_*/*)", "")
	rm $(wildcard ${tools} ${excluded} results_*/*)
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
