package ca.uw.dsg.swc.baselines.dtree;

import java.util.HashSet;
import java.util.Set;

public class DTNode {
    DTNode parent;
    Set<DTNode> children;
    final int val;
    Set<DTNode> nte;
    int size;

    public DTNode(int v) {
        this.parent = null;
        this.children = new HashSet<>();
        this.val = v;
        this.nte = new HashSet<>();
        this.size = 1;
    }

    @Override
    public String toString() {
        return "DTNode{" +
                "val=" + val +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DTNode dtNode = (DTNode) o;

        return val == dtNode.val;
    }

    @Override
    public int hashCode() {
        return val;
    }
}