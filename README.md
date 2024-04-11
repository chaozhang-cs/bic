# Bidirectional Incremental Computation (BIC)
This repository provides the BIC index, an index for processing graph connectivity queries within sliding windows on streaming graphs.

#### Organization

[baselines](https://github.com/chaozhang-cs/bic/tree/main/src/main/java/ca/uw/dsg/swc/baselines) includes the implementation of the state-of-the-art indexes, compared in the experimental study.

[bic](https://github.com/chaozhang-cs/bic/tree/main/src/main/java/ca/uw/dsg/swc/bic) includes the implementation of the BIC index.

[benchmark](https://github.com/chaozhang-cs/bic/tree/main/src/main/java/ca/uw/dsg/swc/benchmark) shows the code for experimental evaluation.

#### Getting Started
1. Clone the project.
2. Execute `mvn clean package`.

#### Reproducibility

1. Download the [datasets_and_workload.tar.gz](https://drive.google.com/file/d/1qZP08kjVP6j1hLJsRAzhbOiBav7f2wPk/view?usp=drive_link), including the datasets and workloads used in the experimental study, and place the tar file under the `benchmark` folder.

2. Execute `tar -czvf rlc-benchmarks.tar.gz`.

3. `benchmark/datasets` and `benchmark/workloads` contains the datasets and workloads.

4. Execute `nohup java -Xms768g -Xmx950g -cp target/swc-1.0-SNAPSHOT.jar ca.uw.dsg.swc.benchmark.BenchmarkRunner &` to reproduce the results. This can take weeks to complete all experiments. Make sure JDK 11 or higher is installed on the tested server. In case the server does not have enough memory for running experiments with extremely large graphs, like Friendster, comment on the settings with such graphs in [BenchmarkRunner](https://github.com/chaozhang-cs/bic/blob/main/src/main/java/ca/uw/dsg/swc/benchmark/BenchmarkRunner.java).

5. The benchmark results are available under the directory `benchmark/results`.

#### Contact
Chao Zhang, chao.zhang@uwaterloo.ca


