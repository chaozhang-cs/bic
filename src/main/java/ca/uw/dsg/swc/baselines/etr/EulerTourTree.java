package ca.uw.dsg.swc.baselines.etr;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.*;

import static ca.uw.dsg.swc.baselines.etr.ImplicitTreapUtils.*;

// For more details about euler tour and euler-tour tree, please see the lecture http://web.stanford.edu/class/archive/cs/cs166/cs166.1166/lectures/17/Slides17.pdf
public class EulerTourTree {
    /**
     * Query whether source and target are in the same connected component.
     * The implementation first gets euler-tour edges (source, v) and (target, u), where vertex v and u are two vertices that are adjacent to source and target, resp.
     * Then, the treap nodes of (source, v) and (target, u) are obtained, and the roots of these two treap nodes are checked to evaluate the query.
     *
     * @param source - the source vertex
     * @param target - the target vertex
     * @return true if source and target are in the same component
     */
    public static boolean connected(InternalVertex source, InternalVertex target) {
        if (source == null || target == null)
            return false;

        EulerTourEdge sourceEdge = source.getAnyEulerTourEdge();
        if (sourceEdge == null)
            return false;
        EulerTourEdge targetEdge = target.getAnyEulerTourEdge();
        if (targetEdge == null)
            return false;

        ImplicitTreapNode<EulerTourEdge> sRoot = sourceEdge.getImplicitTreapNode().getRoot();
        ImplicitTreapNode<EulerTourEdge> tRoot = targetEdge.getImplicitTreapNode().getRoot();
        return sRoot == tRoot;
    }

    /**
     * Link the source to the target in the spanning forest.
     * If source and target are already in the same connected component, then this method did nothing and returns false.
     * Otherwise, this method links the spanning trees of source and target in the forest.
     * The implementation splits the euler tour of the two spanning tree, rotating the euler tour of the spanning trees with source and target as the roots respectively, and building the new euler tour by adding the new edge.
     * For example, consider the following two spanning trees:
     * a            e
     * |            |
     * b            f
     * / \          / \
     * c   d        g   h
     * The euler-tour of the first spanning tree is: abcbdba, and the corresponding treap is: ab, bc, cb, bd, db, ba
     * The euler-tour of the second spanning tree is: efgfhfe, and the corresponding treap is: ef, fg, gf, fh, hf, fe
     * Assuming b is the source, and f is the target, i.e., the new edge is from b to f.
     * Then, the method select a vertex adjacent to b, e.g., c, and a vertex adjacent to f, e.g., g.
     * After that, the two spanning trees are rotated as follows:
     * (ab, bc, cb, bd, db, ba) => ((ab), (bc, cb, bd, db, ba)) => (bc, cb, bd, db, ba, ab)
     * (ef, fg, gf, fh, hf, fe) => ((ef), (fg, gf, fh, hf, fe)) => (fg, gf, fh, hf, fe, ef)
     * Two new euler-tour edges are created: bf and fb.
     * Finally, we have (bc, cb, bd, db, ba, ab) + (bf) + (fg, gf, fh, hf, fe, ef) + fb, where '+' represents concatenation of sequences.
     * Thus, the euler-tour after inserting the new edge is: (bc, cb, bd, db, ba, ab, bf, fg, gf, fh, hf, fe, ef, fb)
     *
     * @param source - the source vertex
     * @param target - the target vertex
     * @return true if source can be linked to target, otherwise false.
     */
    public static boolean link(InternalVertex source, InternalVertex target) {
        if (connected(source, target))
            return false;

        EulerTourEdge sourceEdge = source.getAnyEulerTourEdge(); // get an euler-tour edge (source, v), where v is any vertex
        EulerTourEdge targetEdge = target.getAnyEulerTourEdge(); // get an euler-tour edge (target, u), where u is any vertex

        // split the treap containing source into seq1 and seq2, where seq1 is the sequence before (source, v), and seq2 is the sequence after (source, v) with (source, v) as the first
        SplitTreaps<EulerTourEdge> sourceEdgeSplitTreaps = sourceEdge == null ? new SplitTreaps<>(null, null) : split(sourceEdge.getImplicitTreapNode(), false);

        // split the treap containing source into seq3 and seq4, where seq3 is the sequence before (target, u), and seq3 is the sequence after (target, u) with (target, u) as the first
        SplitTreaps<EulerTourEdge> targetEdgeSplitTreaps = targetEdge == null ? new SplitTreaps<>(null, null) : split(targetEdge.getImplicitTreapNode(), false);

        // create the euler-tour edges for the inserted edge
        EulerTourEdge st = source.addEulerTourEdge(target);
        EulerTourEdge ts = target.addEulerTourEdge(source);

        // rotate the treaps such that the corresponding spanning trees having source and target as their roots respectively
        ImplicitTreapNode<EulerTourEdge> rotatedSourceTreap = join(sourceEdgeSplitTreaps.getSecondSubtreeRoot(), sourceEdgeSplitTreaps.getFirstSubtreeRoot());
        ImplicitTreapNode<EulerTourEdge> rotatedTargetTreap = join(targetEdgeSplitTreaps.getSecondSubtreeRoot(), targetEdgeSplitTreaps.getFirstSubtreeRoot());

        joinTreaps(
                rotatedSourceTreap,
                st.getImplicitTreapNode(),
                rotatedTargetTreap,
                ts.getImplicitTreapNode()
        );
        return true;
    }

    /**
     * Cut the edge from the source to the target in the spanning tree.
     * Assuming the euler-tour edge st is before the euler-tour edge ts in the euler tour.
     * The implementation first split the original euler tour at st, generating T1 and T2, where st is included in T1.
     * Then, T2 is split at ts, generating T2.1 and T2.2, where ts is included in T2.1.
     * After that, st and ts are removed from T1 and T2.1, resp., generating updated-T1 and updated-T2.1.
     * Finally, updated-T1 is joined with T2.2, which is returned together with updated-T2.1 as the two split spanning trees.
     * For example, consider the following spanning tree:
     * a     f
     * |     |
     * b  -  e
     * / \    |
     * c   d   g
     * The original euler-tour is: abcbdbegefeba, and the corresponding treap is: ab, bc, cb, bd, db, be, eg, ge, ef, fe, eb, ba
     * Assuming source is b and target is e.
     * The original euler-tour is first split into the following tours: (ab, bc, cb, bd, db, be) and (eg, ge, ef, fe, eb, ba).
     * Then, (eg, ge, ef, fe, eb, ba) is further split into the following tours: (eg, ge, ef, fe, eb) and (ba).
     * After that, the euler-tour edges be and eb are removed respectively, which generates (ab, bc, cb, bd, db) and (eg, ge, ef, fe).
     * Finally, (ab, bc, cb, bd, db) is joined with (ba), generating (ab, bc, cb, bd, db, ba), which is returned with (eg, ge, ef, fe) as the result of the cutting operation.
     *
     * @param source the source vertex
     * @param target the target vertex
     * @return the two spanning trees that were connected before cutting.
     */
    public static SplitTreaps<EulerTourEdge> cut(InternalVertex source, InternalVertex target) {
        if (source == null || target == null)
            return new SplitTreaps<>(null, null);

        EulerTourEdge stEdge = source.getEulerTourEdge(target);
        EulerTourEdge tsEdge = target.getEulerTourEdge(source);

        // check if source is adjacent to target
        if (stEdge == null || tsEdge == null)
            return null;

        ImplicitTreapNode<EulerTourEdge>
                st = stEdge.getImplicitTreapNode(),
                ts = tsEdge.getImplicitTreapNode();

        // remove the edge from the adjacent list of source and target, resp.
        source.removeEdge(target);
        target.removeEdge(source);

        // let st appear first, and then ts in the corresponding euler-tour
        if (indexOf(st) > indexOf(ts)) {
            ImplicitTreapNode<EulerTourEdge> temp = st;
            st = ts;
            ts = temp;
        }

        // split the euler-tour into (T1, T2), where the euler-tour TO contains the euler-tour edge st, and T2 ts
        ImplicitTreapNode<EulerTourEdge> originalTourUpToSt = split(st).getFirstSubtreeRoot(); // T1

        // split T2 into (T2.1, T2.2), where T2.1 contains the euler-tour edges ts, and T2.2 after ts
        SplitTreaps<EulerTourEdge> splitTreaps = split(ts);
        ImplicitTreapNode<EulerTourEdge> originalTourAfterStUpToTs = splitTreaps.getFirstSubtreeRoot(); // T2.1
        ImplicitTreapNode<EulerTourEdge> originalTourAfterTs = splitTreaps.getSecondSubtreeRoot(); // T2.2

        // remove st in T1
        ImplicitTreapNode<EulerTourEdge> originalTourUpToStWithoutSt = removeLast(originalTourUpToSt); // updated-T1
        // remove ts in T2.1
        ImplicitTreapNode<EulerTourEdge> originalTourAfterStUpToTsWithoutTs = removeLast(originalTourAfterStUpToTs); // updated-T2.1

        return new SplitTreaps<>(
                join(originalTourUpToStWithoutSt, originalTourAfterTs), // the concatenation of updated-T1, and T2.2
                originalTourAfterStUpToTsWithoutTs // updated-T2.1
        );
    }

    /**
     * Compute the euler tours of the spanning trees in a spanning forest.
     * Notice that, this method assumes that the input is a spanning forest, i.e., there does not exist cycles on the graph.
     * After computing the euler tours, this method computes the corresponding implicit treap for each euler tour, i.e., initializing the euler-tour edge of each internal vertex.
     *
     * @param spanningForest an input spanning forest
     * @return a list of roots of implicit treaps for the spanning forest
     */
    public static List<ImplicitTreapNode<EulerTourEdge>> computeEulerTours(InternalGraph spanningForest) {
        List<ImplicitTreapNode<EulerTourEdge>> rootList = new ArrayList<>();
        IntSet vertexSet = new IntOpenHashSet(spanningForest.getVertexSet());
        while (!vertexSet.isEmpty()) {
            // start DFS on a spanning tree with any root to compute the corresponding euler-tour
            Deque<InternalVertex> stack = new ArrayDeque<>();
            stack.push(
                    spanningForest.getInternalVertexOf(
                            vertexSet.iterator().nextInt()
                    )
            );
            ArrayList<InternalVertex> eulerTour = new ArrayList<>();
            while (!stack.isEmpty()) {
                InternalVertex visited = stack.pop();
                eulerTour.add(visited);
                if (vertexSet.remove(visited.getV())) { // the vertex represented by the internal vertex was not visited before
                    for (InternalVertex internalVertex : visited.getAdjacencyList().keySet()) {
                        if (vertexSet.contains(internalVertex.getV())) { // avoid looking back in an undirected graph
                            stack.push(visited);
                            stack.push(internalVertex);
                        }
                    }
                }
            }

            List<ImplicitTreapNode<EulerTourEdge>> list = new ArrayList<>();
            // compute euler-tour edges
            if (eulerTour.size() > 1) {// if the euler-tour contains only one vertex, then the corresponding euler-tour edge is empty
                InternalVertex first = eulerTour.get(0);
                for (int i = 1, num = eulerTour.size(); i < num; i++) {
                    InternalVertex second = eulerTour.get(i);
                    list.add(first.addEulerTourEdge(second).getImplicitTreapNode()); // creating euler-tour edges
                    first = second;
                }
            }

            rootList.add(joinTreaps(list));
        }
        return rootList;
    }
}
