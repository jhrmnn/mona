**!! undocumented and unstable !!**

This project is an effort to ease running calculations in computational chemistry and physics.

The project is "self-documented" via the `example` project. So to start investigating how it works,

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
