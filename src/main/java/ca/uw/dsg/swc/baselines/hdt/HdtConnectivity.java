package ca.uw.dsg.swc.baselines.hdt;


import ca.uw.dsg.swc.baselines.FullyDynamicConnectivity;
import ca.uw.dsg.swc.baselines.etr.*;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


import java.util.*;

import static ca.uw.dsg.swc.baselines.etr.SpanningTree.getEdgeSetOfTree;

public class HdtConnectivity implements FullyDynamicConnectivity {
    private final List<Level> levels; // each level contains a spanning forest F_i, and all non-tree edges of level i. Notice that F_i contains all edges of level at least i
    private final Map<IntIntPair, Integer> edge2Level;

    public HdtConnectivity(int numOfVertices) {
        this.levels = new ArrayList<>();
        this.edge2Level = new HashMap<>();
        int numLevel = (int) Math.floor(Math.log(numOfVertices));
        for (int i = 1; i <= numLevel; i++)
            levels.add(new Level(new InternalGraph(), new InternalGraph()));
    }

    /**
     * Check if source and target are connected.
     *
     * @param source a source vertex
     * @param target a target vertex
     * @return true if source and target are connected, false otherwise.
     */
    @Override
    public boolean connected(int source, int target) {
        InternalGraph l0 = levels.get(0).spanningForest;
        InternalVertex internalSource = l0.getInternalVertexOf(source);
        if (internalSource == null)
            return false;

        InternalVertex internalTarget = l0.getInternalVertexOf(target);
        if (internalTarget == null)
            return false;

        return EulerTourTree.connected(internalSource, internalTarget);
    }

    /**
     * Insert an edge from source to target
     *
     * @param source a source vertex
     * @param target a target vertex
     */
    @Override
    public void insertEdge(int source, int target) {
        IntIntPair sourceTarget = IntIntImmutablePair.of(source, target);
        IntIntPair targetSource = IntIntImmutablePair.of(target, source);
        if (edge2Level.containsKey(sourceTarget)) // check if the edge already exists in this data structure, irrespective of a tree edge or a non-tree edge
            return;

        Level l0 = levels.get(0);
        InternalVertex s = l0.spanningForest.addVertex(source), t = l0.spanningForest.addVertex(target); // add source and target into the spanning forest in case they have not been inserted
        // Assume it is a tree edge and try to link it
        if (!EulerTourTree.link(s, t)) {
            // EulerTourTrees.link() returns false if source has been linked target in the spanning forest, i.e., source and target are in the same component.
            // In this sense, the new inserted edge is a non-tree edge. Every new edge in the HDT algorithm is assigned to level l0.
            l0.nonTreeEdges.addEdge(source, target);
        }

        edge2Level.put(sourceTarget, 0);
        edge2Level.put(targetSource, 0);
    }

    /**
     * Delete an edge from source to target.
     *
     * @param source a source vertex
     * @param target a target vertex
     */
    @Override
    public void deleteEdge(int source, int target) {
        IntIntPair sourceTarget = IntIntImmutablePair.of(source, target);
        int edgeLevel = edge2Level.getOrDefault(sourceTarget, -1);
        if (edgeLevel == -1)
            return;

        // remove the level of the edge between source and target
        edge2Level.remove(sourceTarget);
        edge2Level.remove(IntIntImmutablePair.of(target, source));

        // Each non-tree edge exists only at the non-tree edge store of the corresponding level.
        // Such that if nonTreeEdge.removeEdge(source, target) returns false, then the edge between source and target must be a tree edge.
        if (!levels.get(edgeLevel).nonTreeEdges.removeEdge(source, target)) {
            // it is a tree edge in the follow case
            // plans:
            //  1. delete the tree edge in the forests F0, F1, ..., Fl, where l = edgeLevel
            //  2. search for replacement edge at level Fl, ..., F0.
            //  3. assuming a replacement edge is found at level k.
            //     if k>0, then insert the tree edge at level F0, ..., Fk
            // implementation steps:
            // from l = edgeLevel to F0, for each Fi
            //      cut the edge, and then look for a replacement edge
            // if a replacement edge is eventually found, then from the level of the replacement edge to F0, cut the edge and turn the replacement edge as a tree edge

            ReplacementEdge replacementEdge = replace(source, target, edgeLevel);

            if (replacementEdge != null) { // a replacement edge has been found
                // link the source and target of the replacement edge in the spanning forest
                InternalVertex internalRepSource = levels.get(replacementEdge.level).spanningForest.addVertex(replacementEdge.source),
                        intervalRepTarget = levels.get(replacementEdge.level).spanningForest.addVertex(replacementEdge.target);
                EulerTourTree.link(internalRepSource, intervalRepTarget);
                // remove the replacement edge in the non-tree edge store
                levels.get(replacementEdge.level).nonTreeEdges.removeEdge(replacementEdge.source, replacementEdge.target);

                for (int i = 0; i < replacementEdge.level; i++) {
                    InternalGraph currentSpanningForest = levels.get(i).spanningForest;
                    EulerTourTree.cut( // delete the tree edge between source and target
                            currentSpanningForest.getInternalVertexOf(source),
                            currentSpanningForest.getInternalVertexOf(target)
                    );
                    InternalVertex currentIntervalRepSource = currentSpanningForest.addVertex(replacementEdge.source),
                            currentIntervalRepTarget = currentSpanningForest.addVertex(replacementEdge.target);
                    EulerTourTree.link(currentIntervalRepSource, currentIntervalRepTarget);  // turn the replacement edge as a tree-edge
                }
            }
        }

        for (int i = 0; i < edgeLevel; i++) { // clean vertices of degree 0
            InternalGraph spanningForest = levels.get(i).spanningForest, nonTreeEdges = levels.get(i).nonTreeEdges;
            if (spanningForest.degreeOf(source) == 0)
                spanningForest.removeVertex(source);
            if (nonTreeEdges.degreeOf(target) == 0)
                nonTreeEdges.removeVertex(target);
        }
    }

    /**
     * @return HDT
     */
    @Override
    public String getName() {
        return "HDT";
    }

    private ReplacementEdge replace(int source, int target, int l) { // the returned replacement edge is still a non-tree edge
        //1. cut spanning tree at level l
        //2. search for replacement edge
        //3. if there does not exist a replacement tree at level l, then moving to level l-1 until level 0.
        Level level = levels.get(l);
        InternalGraph spanningForest = level.spanningForest, nonTreeEdges = level.nonTreeEdges;
        IntSet vertexSet = cutTreeEdgeAndGetVertexSetOfSmallerTree(spanningForest, source, target, l);
        for (int v : vertexSet) {
            for (InternalVertex internalVertex : nonTreeEdges.getNeighboursOf(v)) { // iterating over the non-tree edge neighbours of v
                int u = internalVertex.getV();
                if (edge2Level.get(IntIntImmutablePair.of(v, u)) == l) { // only non-tree edges of level l are visited
                    if (vertexSet.contains(u)) { // not a replacement edge, such that push this edge to the next level
                        nonTreeEdges.removeEdge(v, u); // delete the edge at level l
                        levels.get(l + 1).nonTreeEdges.addEdge(v, u);// add the edge at level l+1
                        edge2Level.computeIfPresent(IntIntImmutablePair.of(v, u), (edge, edgeLevel) -> edgeLevel + 1); // increase the edge level by 1
                        edge2Level.computeIfPresent(IntIntImmutablePair.of(u, v), (edge, edgeLevel) -> edgeLevel + 1); // increase the edge level by 1
                    } else { // find a replacement edge, then turn the non-tree edge to tree edge at the current level
                        return new ReplacementEdge(v, u, l);
                    }
                }
            }
        }
        if (l == 0) return null;
        else return replace(source, target, l - 1);
    }

    private IntSet cutTreeEdgeAndGetVertexSetOfSmallerTree(InternalGraph spanningForest, int u, int v, int l) {
        SplitTreaps<EulerTourEdge> splitTreaps = EulerTourTree.cut(spanningForest.getInternalVertexOf(u), spanningForest.getInternalVertexOf(v));
        assert splitTreaps != null;
        ImplicitTreapNode<EulerTourEdge>
                treeU = splitTreaps.getFirstSubtreeRoot(),
                treeV = splitTreaps.getSecondSubtreeRoot();

        IntSet vertexSetOfSmallerTree = new IntOpenHashSet();

        if (treeU == null || treeV == null) { // the euler-tour is one of the following cases: (1) st, ..., ts; (2) ..., st, ts, ...
            vertexSetOfSmallerTree.add(
                    spanningForest.getNeighboursOf(u).isEmpty() ?
                            u :  // u is not adjacent to any vertex
                            v   // v is not adjacent to any vertex
            );
        } else {
            ImplicitTreapNode<EulerTourEdge> smallerTree = ImplicitTreapUtils.sizeOfTreap(treeU) > ImplicitTreapUtils.sizeOfTreap(treeV) ? treeV : treeU;
            // iterating the tree edges
            InternalGraph spanningForestPlus = levels.get(l + 1).spanningForest;
            List<EulerTourEdge> eulerTour = getEdgeSetOfTree(smallerTree);
            for (EulerTourEdge eulerTourEdge : eulerTour) {
                int s = eulerTourEdge.getSource().getV(), t = eulerTourEdge.getTarget().getV();
                IntIntPair treeEdge = IntIntImmutablePair.of(s, t);
                if (edge2Level.getOrDefault(treeEdge, -1) == l) { // according to the HDT algorithm, only edges of level l are pushed to the level l+1
                    edge2Level.computeIfPresent(treeEdge, (edge, level) -> level + 1);
                    edge2Level.computeIfPresent(IntIntImmutablePair.of(t, s), (edge, level) -> level + 1);

                    InternalVertex internalS = spanningForestPlus.addVertex(s), intervalT = spanningForestPlus.addVertex(t);
                    EulerTourTree.link(internalS, intervalT); // add the tree edge at the next level

                }
                vertexSetOfSmallerTree.add(s);
                vertexSetOfSmallerTree.add(t);
            }
        }

        return vertexSetOfSmallerTree;
    }

    private static class Level {
        final InternalGraph spanningForest;
        final InternalGraph nonTreeEdges;

        public Level(InternalGraph spanningForest, InternalGraph nonTreeEdges) {
            this.spanningForest = spanningForest;
            this.nonTreeEdges = nonTreeEdges;
        }
    }

    private static class ReplacementEdge {
        final int source, target;
        final int level;

        public ReplacementEdge(int source, int target, int level) {
            this.source = source;
            this.target = target;
            this.level = level;
        }
    }
}
