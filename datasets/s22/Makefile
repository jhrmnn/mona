sources = suppl-info.txt structures.zip energies.csv article.pdf
doi = 10.1039/b600027d

all: build/energies.csv ref.bib

build/energies.csv: tmp/sanitized.json tmp/unzipped
	mkdir -p build/geoms
	cd build/geoms && python3 ${PWD}/finalize.py \
		${PWD}/$(lastword $^)/*.xyz \
		<${PWD}/$< \
		>${PWD}/$@

tmp/sanitized.json: src/energies.csv | tmp
	cat $< | python3 ../sanitize.py >$@

tmp/unzipped: src/structures.zip | tmp
	mkdir $@
	tar -C $@ -x <$<
	chmod -x $@/*.xyz
	for f in $@/*.xyz; do sed -i "" '/[^[:space:]]/,$$!d' $$f; done

ref.bib:
	doi2bibtex <<<${doi} >$@

tmp:
	mkdir $@

clean:
	-rm -r tmp

distclean: clean
	-rm -r build

web:
	open http://dx.doi.org/${doi}

download: $(addprefix src/,${sources})

src/suppl-info.txt:
	wget -O $@ http://www.rsc.org/suppdata/cp/b6/b600027d/geometries-s22.xyz

src/article.pdf:
	wget -O $@ http://pubs.rsc.org/en/content/articlepdf/2006/cp/b600027d

src/structures.zip:
	wget -O $@ http://www.begdb.com/moldown.php?id=4

src/energies.csv:
	@echo "Go to"
	@echo "http://www.begdb.com/index.php?action=oneDataset&id=4&state=show"
	@echo "click on the csv icon and download the file."
	@read -p "Enter path to csv file: " path; \
		[[ -e "$$path" ]] && cp "$$path" $@ \
		|| { echo "Error: $$path does not exist, exiting..." && exit 1; }
