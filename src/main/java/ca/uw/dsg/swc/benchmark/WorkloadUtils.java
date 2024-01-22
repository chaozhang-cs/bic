package ca.uw.dsg.swc.benchmark;

import ca.uw.dsg.swc.StreamingEdge;
import ca.uw.dsg.swc.baselines.etr.SpanningTree;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

public class WorkloadUtils {
    public static Queue<StreamingEdge> generateStreamingEdges(final List<IntIntPair> edgeList, final int ratePerSecond) {
        Queue<StreamingEdge> ret = new ArrayDeque<>();
        long current = 0L;
        int cnt = 0, i = 0;
        for (IntIntPair e : edgeList) {
            int s = e.firstInt(), t = e.secondInt();
            ret.add(new StreamingEdge(s, t, current + i * 1_000L));
            if (++cnt == ratePerSecond) {
                cnt = 0;
                i++;
            }
        }
        System.gc();
        return ret;
    }

    static List<IntIntPair> readWorkload(String path) {
        System.out.println("Loading workload: " + path);
        List<IntIntPair> ret = null;
        Type querySetType = new TypeToken<List<IntIntImmutablePair>>() {
        }.getType();
        Gson gson = new Gson();
        try {
            Reader reader = new FileReader(path);
            ret = gson.fromJson(reader, querySetType);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    // method for generating workload
    static void generateWorkload() {
        String[] graphs = new String[]{
                "sg-wiki-topcats",
                "sg-com-lj.ungraph",
                "sg-soc-pokec-relationships",
                "sg-stackoverflow",
                "sg-ldbc-sf1k-knows",
                "sg-graph500-25",
                "sg-com-friendster.ungraph",
                "sg-orkut"
        };
        String graphBase = "./benchmark/datasets/", workloadBase = "./benchmark/workloads/";
        for (String graph : graphs) {
            String pathToGraph = graphBase + graph + ".txt";
            String pathToWorkload = workloadBase + graph + ".json";
            generateAndWriteWorkload(pathToGraph, pathToWorkload);
            System.gc();
        }
    }

    private static void writeWorkload(String path, List<IntIntPair> workload) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            FileWriter fileWriter = new FileWriter(path);
            gson.toJson(workload, fileWriter);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void generateAndWriteWorkload(String pathToGraph, String pathToWorkload) {
        System.out.println("Loading graph: " + pathToGraph);
        SpanningTree.IncrementalConnectivity incrementalConnectivity = new SpanningTree.IncrementalConnectivity();
        Scanner scanner;
        Graph<Integer, DefaultEdge> graph = new SimpleGraph<>(DefaultEdge.class);
        try {
            scanner = new Scanner(new File(pathToGraph));
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(",");
                int u = Integer.parseInt(data[0]);
                if (!graph.containsVertex(u))
                    graph.addVertex(u);
                int v = Integer.parseInt(data[1]);
                if (!graph.containsVertex(v))
                    graph.addVertex(v);
                if (u != v) // avoid loops
                    graph.addEdge(u, v);
                incrementalConnectivity.union(u, v);
            }

            System.gc();

            System.out.println("Graph has been loaded: " + pathToGraph);
            System.out.println("Number of vertices: " + graph.vertexSet().size());
            System.out.println("Number of edges: " + graph.edgeSet().size());
            System.out.println("Number of connected components: " + incrementalConnectivity.getCount());

            Random random = new Random(1700249464);
            IntArrayList intArrayList = new IntArrayList(graph.vertexSet());
            List<IntIntPair> vertexAndDegree = new ArrayList<>();
            for (int v : intArrayList)
                vertexAndDegree.add(IntIntPair.of(v, graph.degreeOf(v)));

            vertexAndDegree.sort(Comparator.comparingInt(IntIntPair::secondInt));

            int len = intArrayList.size();
            List<IntIntPair> workload = new ArrayList<>();

            if (incrementalConnectivity.getCount() == 1) {
                addQueries(
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        1_000,
                        len - 100,
                        100,
                        100
                );

                addQueries(
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        2_000,
                        len - 500,
                        500,
                        500
                );

                addQueries(
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        2_000,
                        len - 1000,
                        1000,
                        1000
                );

                addQueries(
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        5_000,
                        0,
                        len,
                        len
                );

            } else {
                addQueries(
                        incrementalConnectivity,
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        1_000,
                        len - 100,
                        100,
                        100
                );

                addQueries(
                        incrementalConnectivity,
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        2_000,
                        len - 500,
                        500,
                        500
                );

                addQueries(
                        incrementalConnectivity,
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        2_000,
                        len - 1000,
                        1000,
                        1000
                );

                addQueries(
                        incrementalConnectivity,
                        workload,
                        vertexAndDegree,
                        random,
                        graph,
                        5_000,
                        0,
                        len,
                        len
                );
            }

            writeWorkload(pathToWorkload, workload);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addQueries(List<IntIntPair> ret, List<IntIntPair> vertexAndDegree, Random random, Graph<Integer, DefaultEdge> graph, int num, int start, int bound1, int bound2) {
        int i = 0;
        while (i++ < num) {
            int s = vertexAndDegree.get(random.nextInt(bound1) + start).firstInt();
            int t = vertexAndDegree.get(random.nextInt(bound2) + start).firstInt();
            if (s != t && !graph.containsEdge(s, t))
                ret.add(IntIntPair.of(s, t));
        }
        System.out.println("Generate query with vertex v1 in [" + start + ", " + (start + bound1) + "]");
        System.out.println("Generate query with vertex v2 in [" + start + ", " + (start + bound2) + "]");
        System.out.println("=================================");
        System.gc();
    }

    private static void addQueries(SpanningTree.IncrementalConnectivity incrementalConnectivity, List<IntIntPair> ret, List<IntIntPair> vertexAndDegree, Random random, Graph<Integer, DefaultEdge> graph, int num, int start, int bound1, int bound2) {
        int i = 0;
        while (i++ < num) {
            int s = vertexAndDegree.get(random.nextInt(bound1) + start).firstInt();
            int t = vertexAndDegree.get(random.nextInt(bound2) + start).firstInt();
            if (s != t && !graph.containsEdge(s, t) && incrementalConnectivity.connected(s, t))
                ret.add(IntIntPair.of(s, t));
        }
        System.out.println("Generate query with vertex v1 in [" + start + ", " + (start + bound1) + "]");
        System.out.println("Generate query with vertex v2 in [" + start + ", " + (start + bound2) + "]");
        System.out.println("=================================");
        System.gc();
    }
}
