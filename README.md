**!! undocumented and unstable !!**

This project is an effort to ease running calculations in computational 
chemistry and physics by

1. Wrapping all the common tasks that one would typically do by hand (copying 
   files, uploading files, downloading files, checking whether a calculation has 
   finished, collecting results, submitting jobs, etc.)
2. Treating a local and remote cluster calculation with the same machinery.
3. Ensuring that outputs of a calculation (a table, a figure, a tarball, you 
   name it) are newer than inputs.

The whole system is built in GNU Make which does a great job here. The idea is 
to have as modular system as possible. Most blocks of code are one to three 
lines. The system basically tries to mimic how one would do stuff **really** by 
hand.

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

If you then pay some time and create a file `example/<remote>_<name>.job.sh` (I 
don't know your submission system, but a have a look in 
[`example/hydra_test.job.sh`][^submit] that works with IBM LoadLeveler) that 
takes care of job submission on the cluster side, you should be able to run 
`make remote_<remote>_<name>` with output like

```
Uploading to hydra...
Connecting to hydra...
python prepare.py
bash ~/bin/submit.sh hydra_test.job.sh
~~~ output of your submitter ~~~
proj.mk:43: *** "Wait till the job finishes, then run make again.".  Stop.
make: *** [RUN/hydra_test_job.log] Error 2
proj.mk:79: recipe for target 'remote_hydra_test' failed
make: *** [remote_hydra_test] Error 2`
```

This warns you that the calculation has not yet finished (it will take a couple 
of seconds). If it was a larger calculation, you could submit additional workers 
with `make submit_<remote>_<name>` to speed it up. When the calculation is 
finished, running `make remote_<remote>_<name>` again should output

```
Uploading to hydra...
Connecting to hydra...
python extract.py
mkdir -p results_hydra_test && mv RUN/results.p results_hydra_test/results.p
Downloading results from hydra...
>f+++++++ results.p
cd results_hydra_test && python ../process.py`
```

If you tried to run `make` before the calculation is finished, it would abort 
with an error. There are many other checks built in.

[^submit]: 
https://github.com/azag0/comp-chem-tools/blob/master/example/hydra_test.job.sh
