package ca.uw.dsg.swc.baselines.etr;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.Set;

public class InternalGraph {
    private final Int2ObjectOpenHashMap<InternalVertex> v2InternalVertex;

    public InternalGraph() {
        v2InternalVertex = new Int2ObjectOpenHashMap<>();
    }

    public InternalVertex addVertex(int v) {
        return v2InternalVertex.computeIfAbsent(v, k -> new InternalVertex(v));
    }

    public void addEdge(int source, int target) {
        InternalVertex internalSource = v2InternalVertex.computeIfAbsent(source, k -> new InternalVertex(source));
        InternalVertex internalTarget = v2InternalVertex.computeIfAbsent(target, k -> new InternalVertex(target));
        internalSource.addEdge(internalTarget);
        internalTarget.addEdge(internalSource);
    }

    public boolean removeEdge(int source, int target) { // returns true if the edge between source and target can be removed, false otherwise
        InternalVertex internalSource = v2InternalVertex.get(source), internalTarget = v2InternalVertex.get(target);

        if (internalSource == null || internalTarget == null)
            return false;


        boolean flag1 = false, flag2 = false;
        if (internalSource.isAdjacentTo(internalTarget)) {
            internalSource.removeEdge(internalTarget);
            flag1 = true;
        }

        if (internalTarget.isAdjacentTo(internalSource)) {
            internalTarget.removeEdge(internalSource);
            flag2 = true;
        }

        return flag1 && flag2;
    }

    public int getNumOfVertices() {
        return v2InternalVertex.size();
    }

    public IntSet getVertexSet() {
        return v2InternalVertex.keySet();
    }

    public InternalVertex getInternalVertexOf(int v) {
        return v2InternalVertex.get(v);
    }

    public boolean containsVertex(int v) {
        return v2InternalVertex.containsKey(v);
    }

    public boolean containsEdge(int source, int target) {
        InternalVertex internalSource = v2InternalVertex.get(source), internalTarget = v2InternalVertex.get(target);
        if (internalSource == null || internalTarget == null)
            return false;
        return internalSource.isAdjacentTo(internalTarget) && internalTarget.isAdjacentTo(internalSource);
    }

    public Set<InternalVertex> getNeighboursOf(int v) {
        InternalVertex internalVertex = v2InternalVertex.get(v);
        Set<InternalVertex> ret = new HashSet<>();
        if (internalVertex != null)
            ret.addAll(internalVertex.getAdjacencyList().keySet());
        return ret;
    }

    public void removeVertex(int v) {
        v2InternalVertex.remove(v);
    }

    @Override
    public String toString() {
        return "Graph{" +
                "v2InternalVertex=" + v2InternalVertex +
                '}';
    }

    public int degreeOf(int v) {
        InternalVertex internalVertex = v2InternalVertex.get(v);
        if (internalVertex == null)
            return 0;
        return internalVertex.getAdjacencyList().size();
    }
}
