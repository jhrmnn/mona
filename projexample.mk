tools += pyexample.py
external += projexample.mk
remotedir ?= ~/calculations

include proj.mk

projexample.mk:
	@rsync -ai ${tooldir}/example/$@ $@
