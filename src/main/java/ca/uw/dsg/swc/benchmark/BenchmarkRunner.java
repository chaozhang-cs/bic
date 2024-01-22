package ca.uw.dsg.swc.benchmark;

import ca.uw.dsg.swc.AbstractSlidingWindowConnectivity;
import ca.uw.dsg.swc.StreamingEdge;
import ca.uw.dsg.swc.baselines.FdcSlidingWindowConnectivity;
import ca.uw.dsg.swc.baselines.dtree.DTreeConnectivity;
import ca.uw.dsg.swc.baselines.etr.EtrConnectivity;
import ca.uw.dsg.swc.baselines.hdt.HdtConnectivity;
import ca.uw.dsg.swc.baselines.naive.DfsConnectivity;
import ca.uw.dsg.swc.baselines.naive.RecalculatingWindowConnectivity;
import ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.jgrapht.alg.util.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BenchmarkRunner {
    static final String BENCHMARK_RESULTS = "./benchmark/results/";
    static final String BENCHMARK_DATASETS = "./benchmark/datasets/";
    static final String BENCHMARK_WORKLOADS = "./benchmark/workloads/";

    static final Map<String, Integer> GRAPH_VERTEX_NUM = Map.of(
            "sg-wiki-topcats", 1791489,
            "sg-com-lj.ungraph", 3997962,
            "sg-soc-pokec-relationships", 1632803,
            "sg-stackoverflow", 2601977,
            "sg-orkut", 3072441,
            "sg-ldbc-sf1k-knows", 3298534,
            "sg-graph500-25", 17062472,
            "sg-com-friendster.ungraph", 65608366
    );

    public static void main(String[] args) {
        // performance evaluation
        throughputRunner();
        latencyRunner();

        // fixed slide interval, varied window sizes
        scalabilityFixedSlideThrExpRunner();
        scalabilityFixedSlideLatencyExpRunner();

        // fixed window size, varied slide intervals
        scalabilityFixedRangeThrExpRunner();
        scalabilityFixedRangeLatencyExpRunner();

        // varied workload sizes
        scalabilityWorkloadThrExpRunner();
        scalabilityWorkloadLatencyExpRunner();
    }


    private static void throughputRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6;
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC",
                "ET-Tree",
                "HDT"
        };
        setupThrExp(
                methods,
                "per-eva",
                "sg-wiki-topcats",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-soc-pokec-relationships",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-com-lj.ungraph",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-stackoverflow",
                List.of(Pair.of(Duration.ofDays(180), Duration.ofDays(9))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-orkut",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-ldbc-sf1k-knows",
                List.of(Pair.of(Duration.ofDays(20), Duration.ofDays(1))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-graph500-25",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-com-friendster.ungraph",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        writeResult(results, BENCHMARK_RESULTS + "throughput-per-eva-" + LocalDateTime.now() + ".txt");
    }

    private static void latencyRunner() {
        String expType = "per-eva";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC",
                "ET-Tree",
                "HDT"
        };
        setupLatencyExp(
                methods,
                expType,
                "sg-wiki-topcats",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-soc-pokec-relationships",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-com-lj.ungraph",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-stackoverflow",
                List.of(Pair.of(Duration.ofDays(180), Duration.ofDays(9)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-orkut",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-ldbc-sf1k-knows",
                List.of(Pair.of(Duration.ofDays(20), Duration.ofDays(1)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
    }

    private static void scalabilityFixedSlideThrExpRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6;
        String expType = "fixed-slide";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                ),
                repeat,
                results
        );
        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityFixedSlideLatencyExpRunner() {
        String expType = "fixed-slide";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );
    }

    private static void scalabilityFixedRangeThrExpRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6;
        String expType = "fixed-range";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                ),
                repeat,
                results
        );
        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityFixedRangeLatencyExpRunner() {
        String expType = "fixed-range";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };
        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );
    }

    private static void scalabilityWorkloadThrExpRunner() {
        List<String> results = new ArrayList<>();

        String expType = "workload";
        int repeat = 6;
        int[] sizes = new int[]{1, 10, 100, 1000, 10000};
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-graph500-25", sizes)
        );

        setupThrExp(
                new String[]{"DFS"},
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-graph500-25", new int[]{1, 10})
        );


        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );

        setupThrExp(
                new String[]{"DFS"},
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );

        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityWorkloadLatencyExpRunner() {
        String expType = "workload";
        int[] sizes = new int[]{1, 10, 100, 1000, 10000};
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-graph500-25", sizes)
        );

        setupLatencyExp(
                new String[]{"DFS"},
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-graph500-25", new int[]{1, 10})
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );

        setupLatencyExp(
                new String[]{"DFS"},
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );
    }

    private static void setupThrExp( // 100 queries
                                     String[] methods,
                                     String expType,
                                     String graph,
                                     List<Pair<Duration, Duration>> rangeAndSlides,
                                     int repeat,
                                     List<String> results) {
        setupThrExp(
                methods,
                expType,
                graph,
                rangeAndSlides,
                repeat,
                results,
                List.of(getWorkLoad(graph, 100))
        );
    }

    private static void setupThrExp( // various sizes of workloads
                                     String[] methods,
                                     String expType,
                                     String graph,
                                     List<Pair<Duration, Duration>> rangeAndSlides,
                                     int repeat,
                                     List<String> results,
                                     List<List<IntIntPair>> workloads) {

        // get graph
        List<StreamingEdge> streamingEdges = GraphUtils.readStreamingGraph(BENCHMARK_DATASETS + graph + ".txt", ",");

        for (List<IntIntPair> workload : workloads) {
            System.out.println("Workload size: " + workload.size());

            if (rangeAndSlides == null)
                return;

            System.out.println("Range and slide" + rangeAndSlides);

            for (String method : methods)
                runThrExp(
                        graph,
                        method,
                        expType,
                        rangeAndSlides,
                        repeat,
                        workload,
                        streamingEdges,
                        results
                );
        }
    }

    private static void setupLatencyExp(
            String[] methods,
            String expType,
            String graph,
            List<Pair<Duration, Duration>> rangeAndSlides) { // 100 queries
        setupLatencyExp(
                methods,
                expType,
                graph,
                rangeAndSlides,
                List.of(getWorkLoad(graph, 100))
        );
    }

    private static void setupLatencyExp(
            String[] methods,
            String expType,
            String graph,
            List<Pair<Duration, Duration>> rangeAndSlides,
            List<List<IntIntPair>> workloads) { // various sizes of workloads
        // get graph
        List<StreamingEdge> streamingEdges = GraphUtils.readStreamingGraph(BENCHMARK_DATASETS + graph + ".txt", ",");

        for (List<IntIntPair> workload : workloads) {
            System.out.println("Workload size: " + workload.size());

            if (rangeAndSlides == null)
                return;

            System.out.println("Range and slide: " + rangeAndSlides);

            for (String method : methods)
                runLatExp(
                        expType,
                        graph,
                        method,
                        rangeAndSlides,
                        workload,
                        streamingEdges
                );
        }
    }

    private static void runThrExp(
            String graph,
            String method,
            String expType,
            List<Pair<Duration, Duration>> rangeSlides,
            int repeat,
            List<IntIntPair> workload,
            List<StreamingEdge> streamingEdges,
            List<String> results) {
        System.out.println("Start " + expType + " throughput experiments for " + method + " on " + graph + " with ranges and slides of " + rangeSlides);
        for (Pair<Duration, Duration> rangeSlide : rangeSlides) {
            Duration range = rangeSlide.getFirst();
            Duration slide = rangeSlide.getSecond();
            for (int i = 0; i < repeat; i++) {
                AbstractSlidingWindowConnectivity slidingWindowConnectivity = getSwc(method, range, slide, workload, graph, streamingEdges.get(0).timeStamp);
                long start = System.nanoTime();
                slidingWindowConnectivity.computeSlidingWindowConnectivity(
                        streamingEdges,
                        initializeOutput(workload.size())
                );
                long end = System.nanoTime();
                String result = graph + "," + expType + "," + method + "," + range.toMillis() + "," + slide.toMillis() + "," + streamingEdges.size() + "," + (end - start) + "," + workload.size();
                System.out.println(result);
                results.add(result);
                System.gc();
            }
        }
    }

    private static void runLatExp(
            String exp,
            String graph,
            String method,
            List<Pair<Duration, Duration>> rangeAndSlides,
            List<IntIntPair> workload,
            List<StreamingEdge> streamingEdges) {

        System.out.println("Warmup");
        getSwc(method, rangeAndSlides.get(0).getFirst(), rangeAndSlides.get(0).getSecond(), workload, graph, streamingEdges.get(0).timeStamp).computeSlidingWindowConnectivity(
                streamingEdges,
                initializeOutput(workload.size())
        );// warm up

        System.out.println("Start latency " + exp + " experiments for " + method + " on " + graph + " with ranges and slides: " + rangeAndSlides);
        for (Pair<Duration, Duration> pair : rangeAndSlides) {
            Duration range = pair.getFirst(), slide = pair.getSecond();
            AbstractSlidingWindowConnectivity slidingWindowConnectivity = getSwc(method, range, slide, workload, graph, streamingEdges.get(0).timeStamp);
            List<Long> result = new ArrayList<>();
            slidingWindowConnectivity.computeSlidingWindowConnectivity(
                    streamingEdges,
                    initializeOutput(workload.size()),
                    result);

            writeLatencyResult(result, BENCHMARK_RESULTS + "latency-" + exp + "-" + pair + "-" + graph + "-" + method + "-" + "workload" + workload.size() + "-" + LocalDateTime.now() + ".txt");
            System.gc();
        }
    }

    private static AbstractSlidingWindowConnectivity getSwc(String method, Duration range, Duration slide, List<IntIntPair> workload, String graph, long first) {
        AbstractSlidingWindowConnectivity ret;
        switch (method) {
            case "DFS":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new DfsConnectivity());
                break;
            case "RWC":
                ret = new RecalculatingWindowConnectivity(range, slide, workload);
                break;
            case "ET-Tree":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new EtrConnectivity());
                break;
            case "HDT":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new HdtConnectivity(GRAPH_VERTEX_NUM.get(graph))); // hdt needs the number of vertices to initialize the number of layers
                break;
            case "D-Tree":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new DTreeConnectivity());
                break;
            case "BIC":
                ret = new BidirectionalIncrementalConnectivity(range, slide, first, workload); // BIC uses first timestamp to initialize chunk
                break;
            default:
                ret = null;
        }
        return ret;
    }

    private static void writeResult(List<String> results, String path) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(path);
            fileWriter.append("graph,experiment,method,range,slide,size,time,workload").append("\n").flush();
            for (String record : results)
                fileWriter.append(record).append("\n").flush();
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeLatencyResult(List<Long> result, String path) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(path);
            for (Long record : result)
                fileWriter.append(record.toString()).append("\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<IntIntPair> getFullWorkLoad(String graph) {
        return WorkloadUtils.readWorkload(BENCHMARK_WORKLOADS + graph + ".json");
    }

    private static List<IntIntPair> getWorkLoad(String graph, int limit) {
        List<IntIntPair> temp = getFullWorkLoad(graph);
        List<IntIntPair> workload = new ArrayList<>();
        Random random = new Random(1700276688);
        for (int i = 0; i < limit; i++) // test a workload of 100 queries
            workload.add(temp.get(random.nextInt(temp.size())));
        return workload;
    }

    private static List<List<IntIntPair>> getWorkLoads(String graph, int[] sizes) {
        List<List<IntIntPair>> ret = new ArrayList<>();
        for (int size : sizes)
            ret.add(getWorkLoad(graph, size));
        return ret;
    }

    static List<List<Boolean>> initializeOutput(int num) {
        List<List<Boolean>> ret = new ArrayList<>();
        for (int i = 0; i < num; i++)
            ret.add(new ArrayList<>());
        return ret;
    }
}
