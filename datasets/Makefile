dirs := l7/ s22/

.PHONY: ${dirs}

all: ${dirs}

${dirs}: | geomlinks
	@cd $@ && ${MAKE}
	
geomlinks: $(addsuffix geomlib.py,${dirs})

%/geomlib.py:
	ln -s ../$(notdir $@) $@

clean:
	-rm */geomlib.py
	for d in ${dirs}; do ${MAKE} -C $$d clean; done

distclean:
	for d in ${dirs}; do ${MAKE} -C $$d distclean; done
