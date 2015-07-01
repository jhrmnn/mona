tools = pyexample.py
external = exampleproj.mk
remotedir = ~/calculations

# a hack to fetch proj.mk to avoid duplicite entries in git repository
FETCH_PROJMK := $(shell rsync -a ../proj.mk .)

include proj.mk

exampleproj.mk:
	@rsync -ai ${tooldir}/example/$@ $@
