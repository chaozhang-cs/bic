package ca.uw.dsg.swc.baselines.etr;

import java.util.HashMap;
import java.util.Map;

public class InternalVertex {
    private final int v;
    private final Map<InternalVertex, EulerTourEdge> adjacencyList;

    public InternalVertex(int v) {
        this.v = v;
        adjacencyList = new HashMap<>();
    }

    public int getV() {
        return v;
    }

    public Map<InternalVertex, EulerTourEdge> getAdjacencyList() {
        return adjacencyList;
    }

    public void addEdge(InternalVertex target) {
        adjacencyList.put(target, null);
    }

    public EulerTourEdge addEulerTourEdge(InternalVertex target) { // only use by the case in euler tour
        EulerTourEdge eulerTourEdge = new EulerTourEdge(this, target);
        adjacencyList.put(target, eulerTourEdge);
        return eulerTourEdge;
    }

    public void removeEdge(InternalVertex target) {
        adjacencyList.remove(target);
    }

    public EulerTourEdge getEulerTourEdge(InternalVertex target) {
        return adjacencyList.get(target);
    }

    public EulerTourEdge getAnyEulerTourEdge() {
        return adjacencyList.isEmpty() ? null : adjacencyList.entrySet().iterator().next().getValue();
    }

    public boolean isAdjacentTo(InternalVertex target) {
        return adjacencyList.containsKey(target);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InternalVertex that = (InternalVertex) o;

        return v == that.v;
    }

    @Override
    public int hashCode() {
        return v;
    }

    @Override
    public String toString() {
        return "InternalVertex{" +
                "v=" + v +
                '}';
    }
}
