inputs = add.in
outputs = results.txt

# a hack to fetch sourced makefiles to avoid duplicite entries in git repository
# normally, both files would be copied to the project directory manually
FETCH_PROJMK := $(shell rsync -a ../{proj.mk,exampleproj.mk} . &>/dev/null)

tooldir = ..
include exampleproj.mk
