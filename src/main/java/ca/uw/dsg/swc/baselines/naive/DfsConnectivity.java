package ca.uw.dsg.swc.baselines.naive;

import ca.uw.dsg.swc.baselines.FullyDynamicConnectivity;
import ca.uw.dsg.swc.baselines.etr.InternalGraph;
import ca.uw.dsg.swc.baselines.etr.InternalVertex;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

public class DfsConnectivity implements FullyDynamicConnectivity {
    private final InternalGraph internalGraph;

    public DfsConnectivity(Graph<Integer, DefaultEdge> graph) {
        internalGraph = new InternalGraph();
        for (DefaultEdge defaultEdge : graph.edgeSet())
            internalGraph.addEdge(graph.getEdgeSource(defaultEdge), graph.getEdgeTarget(defaultEdge));
    }

    public DfsConnectivity() {
        internalGraph = new InternalGraph();
    }

    @Override
    public boolean connected(int source, int target) {
        if (!internalGraph.containsVertex(source) || !internalGraph.containsVertex(target))
            return false;

        InternalVertex internalSource = internalGraph.getInternalVertexOf(source), internalTarget = internalGraph.getInternalVertexOf(target);

        Deque<InternalVertex> stack = new ArrayDeque<>();
        stack.push(internalSource);

        Set<InternalVertex> vSet = new HashSet<>();
        vSet.add(internalSource);

        while (!stack.isEmpty()) {
            InternalVertex visited = stack.pop();
            if (visited.equals(internalTarget))
                return true;
            for (InternalVertex vertex : visited.getAdjacencyList().keySet()) {
                if (vSet.add(vertex))
                    stack.push(vertex);
            }
        }
        return false;
    }

    @Override
    public void insertEdge(int source, int target) {
        internalGraph.addEdge(source, target);
    }

    @Override
    public void deleteEdge(int source, int target) {
        internalGraph.removeEdge(source, target);
        if (internalGraph.degreeOf(source) == 0)
            internalGraph.removeVertex(source);
        if (internalGraph.degreeOf(target) == 0)
            internalGraph.removeVertex(target);
    }

    @Override
    public String getName() {
        return "Naive Connectivity: DFS";
    }
}
