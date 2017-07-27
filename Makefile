$(PWD)/caf: utils/pack.py scripts/caf caflib/*.py caflib/Tools/*.py caflib/Tools/*.tx caflib/extras/*.py
	$^ >$@
