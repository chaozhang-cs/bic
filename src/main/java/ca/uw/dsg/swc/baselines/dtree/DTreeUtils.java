package ca.uw.dsg.swc.baselines.dtree;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectMutablePair;

import java.util.ArrayDeque;
import java.util.Queue;

public class DTreeUtils {
    // The implementation of DTree in Java according to the source code of DTree in Python (https://github.com/qingchen3/D-tree/blob/main/Dtree/Dtree_utils.py)
    // The original implementation has a bug in the query function. The issue is that it is possible to reroot twice, which can produce false negatives.
    // For example, consider a tree rooted at r of size 16 with a child x of size 11, and x has a child y of size 10.
    // Now consider the query (u,v) such that both u and v are successors of y.
    // When computing the root of y, the original query function will get root r and then re-root r to x.
    // After that, the original function will compute x as the root of v, followed by re-rooting x to y.
    // Finally, x and y will be considered as the roots of u and v respectively, which will give False. But, u and v are actually connected.
    // The following version is the original query function shown in https://github.com/qingchen3/D-tree/blob/main/Dtree/Dtree_utils.py from line 347 to line 365

//    @Deprecated
//    public static boolean query(DTNode n_u, DTNode n_v) {
//        DTNode d_u = null;
//        while (n_u.parent != null) {
//            d_u = n_u;
//            n_u = n_u.parent;
//        }
//
//        if (d_u != null && d_u.size > n_u.size / 2)
//            n_u = reRoot(d_u);
//
//
//        DTNode d_v = null;
//        while (n_v.parent != null) {
//            d_v = n_v;
//            n_v = n_v.parent;
//        }
//
//        if (d_v != null && d_v.size > n_v.size / 2)
//            n_v = reRoot(d_v);
//
//
//        return n_u.equals(n_v);
//    }

    public static boolean query(DTNode n_u, DTNode n_v) {
        DTNode d_u = null;
        while (n_u.parent != null) {
            d_u = n_u;
            n_u = n_u.parent;
        }

        DTNode d_v = null;
        while (n_v.parent != null) {
            d_v = n_v;
            n_v = n_v.parent;
        }
        boolean ret = n_u.equals(n_v);

        if (d_u != null && d_u.size > n_u.size / 2)
            reRoot(d_u);

        if (d_v != null && d_v.size > n_v.size / 2)
            reRoot(d_v);

        return ret;
    }

    private static DTNode reRoot(DTNode n_w) {
        if (n_w.parent == null)
            return n_w;

        DTNode ch = n_w;
        DTNode cur = ch.parent;
        n_w.parent = null;

        while (cur != null) {
            DTNode g = cur.parent;

            cur.parent = ch;
            cur.children.remove(ch);
            ch.children.add(cur);

            ch = cur;
            cur = g;
        }

        while (ch.parent != null) {
            ch.size -= ch.parent.size;
            ch.parent.size += ch.size;
            ch = ch.parent;
        }

        return n_w;
    }

    private static void link(DTNode n_u, DTNode r_u, DTNode n_v) {
        n_v.parent = n_u;
        n_u.children.add(n_v);

        DTNode c = n_u;
        DTNode new_root = null;
        while (c != null) {
            c.size += n_v.size;

            if (c.size > (r_u.size + n_v.size) / 2 && new_root == null && c.parent != null)
                new_root = c;

            c = c.parent;
        }
        if (new_root != null)
            reRoot(new_root);
    }

    private static Pair<DTNode, DTNode> unlink(DTNode n_v) {
        // n_v is a non-root node
        DTNode c = n_v;
        while (c.parent != null) {
            c = c.parent;
            c.size -= n_v.size;
        }
        assert n_v.parent != null;
        n_v.parent.children.remove(n_v);
        n_v.parent = null;

        return new ObjectObjectMutablePair<>(n_v, c);  // return n_v, the root node is now 'c'
    }

    public static Pair<DTNode, Integer> findRoot(DTNode node) {
        int dist = 0;
        while (node.parent != null) {
            node = node.parent;
            dist++;
        }
        return new ObjectObjectMutablePair<>(node, dist);
    }

    public static void insertTE(DTNode n_u, DTNode n_v, DTNode r_u, DTNode r_v) {
        // T1 includes v, T2 includes u
        if (r_v.size < r_u.size)
            link(n_u, r_u, reRoot(n_v));
        else
            link(n_v, r_v, reRoot(n_u));
    }

    public static void insertNTE(DTNode r, DTNode n_u, int dist_u, DTNode n_v, int dist_v) {
        // Inserting a non-tree edge

        // Check if n_u is in n_v's nte and n_v is in n_u's nte
        if (n_u.nte.contains(n_v) && n_v.nte.contains(n_u))
            return;

        if (Math.abs(dist_u - dist_v) < 2) {  // No changes to BFS spanning tree
            n_u.nte.add(n_v);
            n_v.nte.add(n_u);
        } else {
            DTNode h, l;
            if (dist_u < dist_v) {
                h = n_v;
                l = n_u;
            } else {
                h = n_u;
                l = n_v;
            }

            int delta = Math.abs(dist_u - dist_v) - 2;
            DTNode c = h;

            for (int i = 1; i < delta; i++)
                c = c.parent;

            c.parent.nte.add(c);
            c.nte.add(c.parent);
            unlink(c);

            link(l, r, reRoot(h));
        }
    }

    public static void deleteNTE(DTNode n_u, DTNode n_v) {
        n_u.nte.remove(n_v);
        n_v.nte.remove(n_u);
    }

    public static void deleteTe(DTNode n_u, DTNode n_v) {
        DTNode ch, root;

        if (n_u.parent == n_v)
            ch = n_u;
        else
            ch = n_v;

        Pair<DTNode, DTNode> pair = unlink(ch);
        ch = pair.first();
        root = pair.second();

        DTNode r_s, r_l;

        if (ch.size < root.size) {
            r_s = ch;
            r_l = root;
        } else {
            r_s = root;
            r_l = ch;
        }

        BFSResult bfsResult = BFSSelect(r_s);
        DTNode n_rs = bfsResult.n_rs, n_rl = bfsResult.n_rl, new_r = bfsResult.new_root;

        if (n_rs == null && n_rl == null) {
            if (new_r != null)
                reRoot(new_r);
        } else {
            assert n_rs != null;
            n_rs.nte.remove(n_rl);
            n_rl.nte.remove(n_rs);
            insertTE(n_rs, n_rl, r_s, r_l);
        }
    }

    private static BFSResult BFSSelect(DTNode r) {
        Queue<DTNode> q = new ArrayDeque<>();
        q.add(r);
        DTNode new_root = null;  // New root for the smaller tree if new_root is not null
        int s = r.size;  // Size of the smaller tree
        int minimum_dist = Integer.MAX_VALUE;
        DTNode n_rs = null;
        DTNode n_rl = null;

        while (!q.isEmpty()) {
            Queue<DTNode> new_q = new ArrayDeque<>();

            while (!q.isEmpty()) {
                DTNode node = q.poll();

                if (s > node.size && node.size > s / 2 && new_root == null)
                    new_root = node;  // New root

                for (DTNode nte : node.nte) {
                    Pair<DTNode, Integer> result = findRoot(nte);
                    DTNode rt = result.first();
                    int dist = result.second();

                    if (rt.equals(r))   // This non-tree edge is included in the smaller tree.
                        continue;

                    if (dist < minimum_dist) {
                        minimum_dist = dist;
                        n_rl = nte;  // In the larger tree
                        n_rs = node;  // In the smaller tree
                    }
                }

                q.addAll(node.children);
            }

            q = new_q;
        }

        return new BFSResult(n_rs, n_rl, new_root);
    }

    private static class BFSResult {
        DTNode n_rs;
        DTNode n_rl;
        DTNode new_root;

        BFSResult(DTNode n_rs, DTNode n_rl, DTNode new_root) {
            this.n_rs = n_rs;
            this.n_rl = n_rl;
            this.new_root = new_root;
        }
    }
}
