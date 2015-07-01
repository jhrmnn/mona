tools = pyexample.py
external = exampleproj.mk
remotedir = ~/calculations

include proj.mk

exampleproj.mk:
	@rsync -ai ${tooldir}/example/$@ $@
