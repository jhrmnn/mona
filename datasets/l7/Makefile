sources = suppl-info.pdf structures.zip energies.csv article.pdf
doi = 10.1021/ct400036b

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

src/suppl-info.pdf:
	wget -O $@ http://pubs.acs.org/doi/suppl/10.1021/ct400036b/suppl_file/ct400036b_si_002.pdf

src/article.pdf:
	wget -O $@ http://pubs.acs.org/doi/pdfplus/10.1021/ct400036b

src/structures.zip:
	wget -O $@ http://www.begdb.com/moldown.php?id=40

src/energies.csv:
	@echo "Go to"
	@echo "http://www.begdb.com/index.php?action=oneDataset&id=40&state=show"
	@echo "click on the csv icon and download the file."
	@read -p "Enter path to csv file: " path; \
		[[ -e "$$path" ]] && cp "$$path" $@ \
		|| { echo "Error: $$path does not exist, exiting..." && exit 1; }
