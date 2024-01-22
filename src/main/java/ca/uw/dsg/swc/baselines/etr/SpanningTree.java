package ca.uw.dsg.swc.baselines.etr;


import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.jgrapht.Graph;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.DefaultEdge;

import java.util.*;

public class SpanningTree {
    public static Pair<InternalGraph, InternalGraph> initializeForestAndNonTreeEdges(Graph<Integer, DefaultEdge> graph) {
        final InternalGraph spanningForest = new InternalGraph(), nonTreeEdges = new InternalGraph();
        graph.vertexSet().forEach(v -> {
            spanningForest.addVertex(v);
            nonTreeEdges.addVertex(v);
        });

        IntSet vertexSet = spanningForest.getVertexSet();
        IncrementalConnectivity incrementalConnectivity = new IncrementalConnectivity(vertexSet);

        for (DefaultEdge defaultEdge : graph.edgeSet()) {
            Pair<Integer, Integer> edge = Pair.of(graph.getEdgeSource(defaultEdge), graph.getEdgeTarget(defaultEdge));
            if (edge.getFirst() == null || edge.getSecond() == null)
                continue;

            if (incrementalConnectivity.connected(edge.getFirst(), edge.getSecond())) { // source and target are connected
                if (!spanningForest.containsEdge(edge.getFirst(), edge.getSecond())) // if the edge (source, target) is not included in spanning forest
                    nonTreeEdges.addEdge(edge.getFirst(), edge.getSecond()); // inserting (source, target) as a non-tree edges
                // notice that the case that (source, target) are connected but (source, target) has already been included in spanning forest, the edge is not inserted as a non-tree edge
                // such a case can happen, for instance an edge appears twice, or both (u,v) and (v,u) exist as input
            } else {
                // source and target are not connected, then (source, target) is a tree edge
                incrementalConnectivity.union(edge.getFirst(), edge.getSecond());
                spanningForest.addEdge(edge.getFirst(), edge.getSecond());
            }
        }

        return Pair.of(spanningForest, nonTreeEdges);
    }

    public static List<EulerTourEdge> getEdgeSetOfTree(ImplicitTreapNode<EulerTourEdge> root) {
        List<EulerTourEdge> eulerTour = new ArrayList<>();
        inOrderTraversal(root, eulerTour);
        return eulerTour;
    }

    private static void inOrderTraversal
            (ImplicitTreapNode<EulerTourEdge> node, List<EulerTourEdge> eulerTour) {
        if (node == null)
            return;
        inOrderTraversal(node.getLeftChild(), eulerTour);
        eulerTour.add(node.getData());
        inOrderTraversal(node.getRightChild(), eulerTour);
    }

    /**
     * This implementation is based on the weighted quick union find with path compression.
     * Please see <a href="https://algs4.cs.princeton.edu/15uf">Section 1.5</a> of Algorithms, 4th Edition by Robert Sedgewick and Kevin Wayne for additional details.
     */
    public static class IncrementalConnectivity {
        private final Int2IntOpenHashMap parent; // map a vertex to its parent, and if a vertex is a root, then the vertex is mapped to itself
        private final Int2IntOpenHashMap size; // if a vertex is a root, then the vertex is mapped to size of the tree
        private int count; // number of connected components

        /**
         * Initialize an empty incremental connectivity with a set of {@code vertices}.
         * Initially, each vertex is in its own component.
         *
         * @param vertices a set of vertices
         */
        public IncrementalConnectivity(IntSet vertices) {
            count = vertices.size();
            parent = new Int2IntOpenHashMap();
            size = new Int2IntOpenHashMap();
            for (int v : vertices) {
                parent.put(v, v);
                size.put(v, 1);
            }
        }

        /**
         * Initialize an empty incremental connectivity without a set of vertices.
         */
        public IncrementalConnectivity() {
            count = 0;
            parent = new Int2IntOpenHashMap();
            size = new Int2IntOpenHashMap();
        }

        private void addVertex(int v) {
            parent.putIfAbsent(v, v);
            size.putIfAbsent(v, 1);
            count++;
        }

        private void insertVerticesIfAbsent(int source, int target) {
            if (!parent.containsKey(source))
                addVertex(source);
            if (!parent.containsKey(target))
                addVertex(target);
        }

        /**
         * Returns true if source and target are in the same component.
         * If source or target was not included in the data structure, they will not be inserted during executing this method.
         *
         * @param source a vertex
         * @param target a vertex
         * @return {@code true} if {@code source} and {@code target} are in the same component;
         * {@code false} otherwise.
         */
        public boolean connected(int source, int target) {
            if (!parent.containsKey(source) || !parent.containsKey(target))
                return false;
            return find(source) == find(target);
        }

        /**
         * Links the source vertex to the target vertex.
         * Returns true if the data structure changes after linking, false otherwise.
         * If source or target was not included in the data structure, they will be first inserted before linking.
         *
         * @param source a source vertex
         * @param target a target vertex
         * @return true if the data structure changes after linking, false otherwise.
         */
        public boolean union(int source, int target) { // aka the union method
            insertVerticesIfAbsent(source, target);
            int rootOfSource = find(source);
            int rootOfTarget = find(target);
            if (rootOfSource == rootOfTarget) return false;

            int updatedSize = size.get(rootOfTarget) + size.get(rootOfSource);

            if (size.get(rootOfSource) < size.get(rootOfTarget)) {
                parent.put(rootOfSource, rootOfTarget);
                size.put(rootOfTarget, updatedSize);
            } else {
                parent.put(rootOfTarget, rootOfSource);
                size.put(rootOfSource, updatedSize);
            }

            count--; // a new connected component has been found
            return true;
        }

        private int find(final int v) {
            // compute the root of v
            int tempRoot = parent.get(v);
            while (tempRoot != parent.get(tempRoot))
                tempRoot = parent.get(tempRoot);

            // optimization
            int u = v;
            final int root = tempRoot;
            while (u != root) {
                int temp = parent.get(u);
                parent.put(u, root);
                u = temp;
            }
            return root;
        }

        /**
         * Returns the number of components
         *
         * @return the number of components.
         */
        public int getCount() {
            return count;
        }
    }
}
