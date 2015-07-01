# @ job_type = parallel
# @ node_usage = not_shared
# @ output = RUN/hydra_test_job.log
# @ error = RUN/hydra_test_job.log
# @ tasks_per_node = 20
# @ resources = ConsumableCpus(1)
# @ network.MPI = sn_all,not_shared,us
# @ shell = /bin/bash
# @ notification = never
# @ environment = JOB_ID=$(jobid)
# @ node = 1
# @ wall_clock_limit = 600
# @ node_resources = ConsumableMemory(56gb)
# @ queue

python worker.py RUN $JOB_ID $HOME/scratch/runs
