package ca.uw.dsg.swc.benchmark;

import ca.uw.dsg.swc.StreamingEdge;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.jgrapht.Graph;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.DefaultEdge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import static ca.uw.dsg.swc.benchmark.WorkloadUtils.generateStreamingEdges;

public class GraphUtils {
    public static final int RATE_PER_SECOND = 100;

    // method for preparing streaming graphs
    public static void prepareDatasets() {
        getGraphAndAddTimeStamps(
                "./benchmark/datasets/com-dblp.ungraph.txt",
                "./benchmark/datasets/sg-com-dblp.ungraph.txt",
                0,
                1,
                "\t",
                "#"
        );

        getGraphAndAddTimeStamps(
                "./benchmark/datasets/com-lj.ungraph.txt",
                "./benchmark/datasets/sg-com-lj.ungraph.txt",
                0,
                1,
                "\t",
                "#"
        );

        getGraphAndAddTimeStamps(
                "./benchmark/datasets/soc-pokec-relationships.txt",
                "./benchmark/datasets/sg-soc-pokec-relationships.txt",
                0,
                1,
                "\t",
                "#"
        );
        getGraphAndAddTimeStamps(
                "./benchmark/datasets/wiki-topcats.txt",
                "./benchmark/datasets/sg-wiki-topcats.txt",
                0,
                1,
                " ",
                "#"
        );
        getGraphAndAddTimeStamps(
                "./benchmark/datasets/orkut.txt",
                "./benchmark/datasets/sg-orkut.txt",
                0,
                1,
                " ",
                "%"
        );
        getGraphAndAddTimeStamps(
                "./benchmark/datasets/com-friendster.ungraph.txt",
                "./benchmark/datasets/sg-com-friendster.ungraph.txt",
                0,
                1,
                "\t",
                "#"
        );

        processStackOverflow(
                "./benchmark/datasets/sx-stackoverflow.txt",
                "./benchmark/datasets/sg-sx-stackoverflow.txt"
        );

        processLDBCData(
                "./benchmark/datasets/ldbc-sf1k-knows.txt",
                "./benchmark/datasets/sg-ldbc-sf1k-knows.txt");
    }

    public static void processStackOverflow(String read, String write) {
        Scanner scanner;
        List<StreamingEdge> streamingEdgeList = new ArrayList<>();
        try {
            scanner = new Scanner(new File(read));
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(" ");
                streamingEdgeList.add(
                        new StreamingEdge(
                                Integer.parseInt(data[0]),
                                Integer.parseInt(data[1]),
                                Long.parseLong(data[2]) * 1_000
                        ));
            }

            streamingEdgeList.sort(Comparator.comparingLong(s -> s.timeStamp));
            writeStreamingGraph(write, streamingEdgeList);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeStreamingGraph(String path, Collection<StreamingEdge> streamingEdges) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(path);
            int i = 0;
            for (StreamingEdge streamingEdge : streamingEdges) {
                fileWriter
                        .append(String.valueOf(streamingEdge.source)).append(",")
                        .append(String.valueOf(streamingEdge.target)).append(",")
                        .append(String.valueOf(streamingEdge.timeStamp))
                        .append("\n");
                if (++i % 1_000_000 == 0)
                    fileWriter.flush();
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<StreamingEdge> readStreamingGraph(String path, String split) {
        List<StreamingEdge> streamingEdges;
        Scanner scanner;
        int loop = 0, multiEdge = 0;

        try {
            scanner = new Scanner(new File(path));
            streamingEdges = new ArrayList<>();
            System.out.println("Loading graph: " + path);
            HashSet<IntIntPair> set = new HashSet<>();
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(split);
                int source = Integer.parseInt(data[0]), target = Integer.parseInt(data[1]);
                if (source == target) { // avoid loop
                    loop++;
                    continue;
                }
                if (source > target) { // avoid multiple edge between vertices
                    if (!set.add(IntIntPair.of(source, target))) {
                        multiEdge++;
                        continue;
                    }
                } else {
                    if (!set.add(IntIntPair.of(target, source))) {
                        multiEdge++;
                        continue;
                    }
                }
                streamingEdges.add(new StreamingEdge(source, target, Long.parseLong(data[2])));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Num of streaming edges in the queue: " + streamingEdges.size());
        System.out.println("Num of loops: " + loop);
        System.out.println("Num of multi-edges: " + multiEdge);
        return streamingEdges;
    }

    public static List<StreamingEdge> readStreamingGraph2(String path, String split, int limit) {
        List<StreamingEdge> streamingEdges;
        Scanner scanner;
        int loop = 0, multiEdge = 0;

        int i = 0;

        try {
            scanner = new Scanner(new File(path));
            streamingEdges = new ArrayList<>();
            System.out.println("Loading graph: " + path);
            HashSet<IntIntPair> set = new HashSet<>();
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(split);
                int source = Integer.parseInt(data[0]), target = Integer.parseInt(data[1]);
                if (source == target) { // avoid loop
                    loop++;
                    continue;
                }
                if (source > target) { // avoid multiple edge between vertices
                    if (!set.add(IntIntPair.of(source, target))) {
                        multiEdge++;
                        continue;
                    }
                } else {
                    if (!set.add(IntIntPair.of(target, source))) {
                        multiEdge++;
                        continue;
                    }
                }
                streamingEdges.add(new StreamingEdge(source, target, Long.parseLong(data[2])));
                if (i++ == limit)
                    break;
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Num of streaming edges in the queue: " + streamingEdges.size());
        System.out.println("Num of loops: " + loop);
        System.out.println("Num of multi-edges: " + multiEdge);
        return streamingEdges;
    }

    public static void getGraphAndAddTimeStamps(String read, String write, int sourceIndex, int targetIndex, String split, String skip) {
        List<IntIntPair> edgeList;
        Scanner scanner;
        try {
            scanner = new Scanner(new File(read));
            edgeList = new ArrayList<>();
            System.out.println("Loading graph: " + read);
            int i = 0;
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                if (line.startsWith(skip))
                    continue;
                String[] data = line.split(split);
                edgeList.add(IntIntPair.of(
                        Integer.parseInt(data[sourceIndex]),
                        Integer.parseInt(data[targetIndex])
                ));
                if (++i % 1_000_000 == 0)
                    System.out.println("Number of edges loaded: " + i);
            }
            System.gc();
            System.out.println("Adding timestamps");
            Queue<StreamingEdge> input = generateStreamingEdges(edgeList, RATE_PER_SECOND);
            edgeList = null;
            System.gc();
            System.out.println("Writing graph: " + write);
            writeStreamingGraph(
                    write,
                    input
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <V> Collection<Pair<V, V>> getEdgeList(Graph<V, DefaultEdge> graph) {
        Set<Pair<V, V>> ret = new HashSet<>();
        for (DefaultEdge edge : graph.edgeSet()) {
            Pair<V, V> pair = Pair.of(graph.getEdgeSource(edge), graph.getEdgeTarget(edge));
            Pair<V, V> pair2 = Pair.of(graph.getEdgeTarget(edge), graph.getEdgeSource(edge));
            if (!ret.contains(pair) && !ret.contains(pair2))
                ret.add(
                        Pair.of(
                                graph.getEdgeSource(edge),
                                graph.getEdgeTarget(edge)
                        )
                );
        }

        for (V v : graph.vertexSet())
            if (graph.degreeOf(v) == 0)
                ret.add(Pair.of(v, null));
        return ret;
    }

    public static void processLDBCData(String read, String write) {
        List<StreamingEdge> streamingEdges;
        Scanner scanner;
        int loop = 0, multiEdge = 0;

        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        try {
            scanner = new Scanner(new File(read));
            streamingEdges = new ArrayList<>();
            System.out.println("Loading graph: " + read);
            HashSet<IntIntPair> set = new HashSet<>();

            DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;// parsing timestamps

            System.out.println(scanner.nextLine());

            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split("\\|");

                long s;
                try {
                    s = Long.parseLong(data[1]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    continue;
                }

                long t;
                try {
                    t = Long.parseLong(data[2]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    continue;
                }
                long ts;
                try {
                    ts = OffsetDateTime.parse(data[0], formatter).toInstant().getEpochSecond() * 1_000;
                } catch (DateTimeParseException e) {
                    System.out.println(e.getMessage());
                    continue;
                }

                int source = map.computeIfAbsent(s, k -> map.size());
                int target = map.computeIfAbsent(t, k -> map.size());

                if (source == target) { // avoid loop
                    loop++;
                    continue;
                }
                if (source > target) { // avoid multiple edge between vertices
                    if (!set.add(IntIntPair.of(source, target))) {
                        multiEdge++;
                        continue;
                    }
                } else {
                    if (!set.add(IntIntPair.of(target, source))) {
                        multiEdge++;
                        continue;
                    }
                }
                streamingEdges.add(
                        new StreamingEdge(
                                source,
                                target,
                                ts
                        )
                );
            }

            streamingEdges.sort(Comparator.comparingLong(se -> se.timeStamp));
            writeStreamingGraph(write, streamingEdges);

            System.out.println("Num of streaming edges in the queue: " + streamingEdges.size());
            System.out.println("Num of loops: " + loop);
            System.out.println("Num of multi-edges: " + multiEdge);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void processSemanticScholarData(String read, String write) {
        Scanner scanner;
        try {
            scanner = new Scanner(new File(read));

            int loop = 0, multiEdge = 0;
            Long2IntOpenHashMap map = new Long2IntOpenHashMap();

            System.out.println("Loading graph: " + read);

            HashSet<IntIntPair> set = new HashSet<>();

            List<IntIntPair> edgeList = new ArrayList<>();

            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(" ");

                long s;
                try {
                    s = Long.parseLong(data[0]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    continue;
                }

                long t;
                try {
                    t = Long.parseLong(data[1]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    continue;
                }

                int source = map.computeIfAbsent(s, k -> map.size());
                int target = map.computeIfAbsent(t, k -> map.size());

                if (source == target) { // avoid loop
                    loop++;
                    continue;
                }

                if (source > target) { // avoid multiple edge between vertices
                    if (!set.add(IntIntPair.of(source, target))) {
                        multiEdge++;
                        continue;
                    }
                } else {
                    if (!set.add(IntIntPair.of(target, source))) {
                        multiEdge++;
                        continue;
                    }
                }

                edgeList.add(IntIntPair.of(source, target));
            }

            set = null;

            Queue<StreamingEdge> streamingEdges = generateStreamingEdges(edgeList, RATE_PER_SECOND);

            edgeList = null;

            writeStreamingGraph(write, streamingEdges);

            System.out.println("Num of streaming edges in the queue: " + streamingEdges.size());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
