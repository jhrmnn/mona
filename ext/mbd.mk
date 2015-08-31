external += projmbd.mk run_mbd.sh
remotedir ?= ~/calculations
excluded += run_mbd.sh

include proj.mk

projmbd.mk:
	@rsync -ai ${tooldir}/$@ $@

run_mbd.sh:
	@rsync -ai ~/bin/$@ $@
