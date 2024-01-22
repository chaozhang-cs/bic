package ca.uw.dsg.swc.baselines.dtree;

import ca.uw.dsg.swc.baselines.FullyDynamicConnectivity;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class DTreeConnectivity implements FullyDynamicConnectivity {
    private final Int2ObjectOpenHashMap<DTNode> nodeHashMap;

    public DTreeConnectivity() {
        this.nodeHashMap = new Int2ObjectOpenHashMap<>();
    }


    @Override
    public boolean connected(int source, int target) {
        DTNode s = nodeHashMap.get(source);
        if (s == null)
            return false;

        DTNode t = nodeHashMap.get(target);
        if (t == null)
            return false;

        return DTreeUtils.query(s, t);
    }

    @Override
    public void insertEdge(int source, int target) {
        DTNode s = nodeHashMap.computeIfAbsent(source, k -> new DTNode(source));
        DTNode t = nodeHashMap.computeIfAbsent(target, k -> new DTNode(target));

        Pair<DTNode, Integer> sourcePair = DTreeUtils.findRoot(s);
        DTNode s_r = sourcePair.first();
        int s_distance = sourcePair.second();

        Pair<DTNode, Integer> targetPair = DTreeUtils.findRoot(t);
        DTNode t_r = targetPair.first();
        int t_distance = targetPair.second();

        if (!s_r.equals(t_r)) { // not connected
            DTreeUtils.insertTE(s, t, s_r, t_r);
        } else { // connected
            if (!(s.parent == t || t.parent == s)) {
                DTreeUtils.insertNTE(s_r, s, s_distance, t, t_distance);
            }
        }
    }

    @Override
    public void deleteEdge(int source, int target) {
        DTNode s = nodeHashMap.get(source);
        DTNode t = nodeHashMap.get(target);

        if (s == null || t == null) // in the case of trying to delete an edge of vertices that have already been deleted when the window is sliding
            return;

        if (s.parent == t || t.parent == s) { // deleting tree edge
            DTreeUtils.deleteTe(s, t);
        } else {  // deleting tree edge
            DTreeUtils.deleteNTE(s, t);
        }


        // Delete the nodes if they are not adjacent to any other nodes
        if (s.nte.isEmpty() && s.parent == null && s.children.isEmpty())
            nodeHashMap.remove(source, s);
        if (t.nte.isEmpty() && t.parent == null && t.children.isEmpty())
            nodeHashMap.remove(target, t);
    }

    @Override
    public String getName() {
        return "DTree Connectivity";
    }
}
