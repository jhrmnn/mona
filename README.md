**!! undocumented and unstable !!**

This project is an effort to ease running calculations in computational 
chemistry and physics by

1. Wrapping all the common tasks that one would typically do by hand (copying 
   files, uploading files, downloading files, checking whether a calculation has 
   finished, collecting results, submitting jobs, etc.)
2. Treating a local and remote cluster calculation with the same machinery.
3. Ensuring that outputs of a calculation (a table, a figure, a tarball, you 
   name it) are newer than inputs.

The project is "self-documented" via the `example` project. So to start 
investigating how it works,

```bash
cd example
make
```

This should output

```
>f+++++++ pyexample.py
>f+++++++ dispatcher.py
>f+++++++ worker.py
python prepare.py
python worker.py RUN 1 >RUN/local_job.log
python extract.py
mkdir -p results_local && mv RUN/results.p results_local/results.p
cd results_local && python ../process.py
```

Running `make` again after that should output

```
make[1]: 'results_local/results.txt' is up to date.
```
