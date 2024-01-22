package ca.uw.dsg.swc.baselines.etr;

import ca.uw.dsg.swc.baselines.FullyDynamicConnectivity;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


import java.util.List;

import static ca.uw.dsg.swc.baselines.etr.SpanningTree.getEdgeSetOfTree;

public class EtrConnectivity implements FullyDynamicConnectivity {

    public final InternalGraph spanningForest, nonTreeEdges;
    public EtrConnectivity() {
        spanningForest = new InternalGraph();
        nonTreeEdges = new InternalGraph();
    }

    @Override
    public boolean connected(int source, int target) {
        InternalVertex internalSource = spanningForest.getInternalVertexOf(source);
        if (internalSource == null)
            return false;
        InternalVertex internalTarget = spanningForest.getInternalVertexOf(target);
        if (internalTarget == null)
            return false;
        return EulerTourTree.connected(internalSource, internalTarget);
    }


    @Override
    public void insertEdge(int source, int target) {
        if (!nonTreeEdges.containsEdge(source, target) && !spanningForest.containsEdge(source, target)) { // (source, target) is not included in neither of nonTreeEdges and spanningForest
            InternalVertex s = spanningForest.addVertex(source);
            InternalVertex t = spanningForest.addVertex(target);
            if (!EulerTourTree.link(s, t))  // source and target have already been connected
                nonTreeEdges.addEdge(source, target);
        }
    }

    @Override
    public void deleteEdge(int source, int target) {
        if (!nonTreeEdges.containsEdge(source, target) && !spanningForest.containsEdge(source, target)) // deleting a non-existing edge
            return;
        if (!nonTreeEdges.removeEdge(source, target)) { // try to remove non-tree edges
            // the following part is for the case of tree edge
            IntSet vSet = cutTreeEdgeAndGetVertexSetOfSmallerTree(spanningForest, source, target);
            for (int v : vSet) {
                for (InternalVertex internalVertex : nonTreeEdges.getNeighboursOf(v)) {
                    int u = internalVertex.getV();
                    if (!vSet.contains(u)) {// replacement edge (v, u) is found
                        InternalVertex intervalV = spanningForest.addVertex(v), intervalU = spanningForest.addVertex(u);
                        EulerTourTree.link(intervalV, intervalU);
                        nonTreeEdges.removeEdge(v, u); // remove the replacement edge from the non-tree edge store
                        return;
                    }
                }
            }
        }

        if (nonTreeEdges.degreeOf(source) == 0)
            nonTreeEdges.removeVertex(source);

        if (nonTreeEdges.degreeOf(target) == 0)
            nonTreeEdges.removeVertex(target);

        if (spanningForest.degreeOf(source) == 0)
            spanningForest.removeVertex(source);

        if (spanningForest.degreeOf(target) == 0)
            spanningForest.removeVertex(target);
    }

    private IntSet cutTreeEdgeAndGetVertexSetOfSmallerTree(InternalGraph spanningForest, int u, int v) {
        SplitTreaps<EulerTourEdge> splitTreaps = EulerTourTree.cut(spanningForest.getInternalVertexOf(u), spanningForest.getInternalVertexOf(v));
        assert splitTreaps != null;
        ImplicitTreapNode<EulerTourEdge>
                treeU = splitTreaps.getFirstSubtreeRoot(),
                treeV = splitTreaps.getSecondSubtreeRoot();
        IntSet vSet = new IntOpenHashSet();
        if (treeU == null || treeV == null) {
            vSet.add(spanningForest.getNeighboursOf(u).isEmpty() ? u : v);
        } else {
            ImplicitTreapNode<EulerTourEdge> smallerTree = ImplicitTreapUtils.sizeOfTreap(treeU) > ImplicitTreapUtils.sizeOfTreap(treeV) ? treeV : treeU;
            List<EulerTourEdge> eulerTourEdges = getEdgeSetOfTree(smallerTree);
            for (EulerTourEdge eulerTourEdge : eulerTourEdges) {
                vSet.add(eulerTourEdge.getSource().getV());
                vSet.add(eulerTourEdge.getTarget().getV());
            }
        }
        return vSet;
    }

    @Override
    public String getName() {
        return "ETR Connectivity";
    }

    public void print(int v) {
        InternalVertex internalVertex = spanningForest.getInternalVertexOf(v);
        ImplicitTreapUtils.print(internalVertex.getAnyEulerTourEdge().getImplicitTreapNode());
    }
}
