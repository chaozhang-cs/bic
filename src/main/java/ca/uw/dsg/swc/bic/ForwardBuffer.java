package ca.uw.dsg.swc.bic;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIntPair;

import java.util.*;

import static ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity.EMPTY_ROOT;

public class ForwardBuffer {
    private final Int2ObjectOpenHashMap<TreeNode> vertex2Node; // map vertex to its node in union-find tree
    private final RootAndChild rootAndChildInF;

    private final Deque<TreeNode> treeNodeQueue;

    private int[] sources, targets;
    private TreeNode[] sourceNodes, targetNodes;

    public ForwardBuffer() {
        vertex2Node = new Int2ObjectOpenHashMap<>();
        rootAndChildInF = new RootAndChild();
        treeNodeQueue = new ArrayDeque<>(100);
    }

    public ForwardBuffer(List<IntIntPair> workload) {
        vertex2Node = new Int2ObjectOpenHashMap<>();
        rootAndChildInF = new RootAndChild();
        treeNodeQueue = new ArrayDeque<>(100);

        int len = workload.size();
        sources = new int[len];
        targets = new int[len];
        sourceNodes = new TreeNode[len];
        targetNodes = new TreeNode[len];
        int i = 0;
        for (IntIntPair st : workload) {
            sources[i] = st.firstInt();
            targets[i++] = st.secondInt();
        }

    }

    public int insertEdge(int source, int target) {
        TreeNode sourceTreeNode = vertex2Node.computeIfAbsent(source, k -> new TreeNode(source));
        TreeNode targetTreeNode = vertex2Node.computeIfAbsent(target, k -> new TreeNode(target));
        TreeNode rootOfSource = find(sourceTreeNode);
        TreeNode rootOfTarget = find(targetTreeNode);
        if (rootOfSource != rootOfTarget)
            return union(rootOfSource, rootOfTarget).v; // return the root after union
        else
            return rootOfSource.v; // the case that source and target have already been connected, return their root
    }

    RootAndChild insertEdgeAndGetRoots(int source, int target) {
        TreeNode sourceTreeNode = vertex2Node.computeIfAbsent(source, k -> new TreeNode(source));
        TreeNode targetTreeNode = vertex2Node.computeIfAbsent(target, k -> new TreeNode(target));
        TreeNode rootOfSource = find(sourceTreeNode);
        TreeNode rootOfTarget = find(targetTreeNode);
        if (rootOfSource != rootOfTarget) {
            TreeNode root = union(rootOfSource, rootOfTarget);
            TreeNode child =
                    root == rootOfSource
                            ? rootOfTarget
                            : rootOfSource; // child.v will be used to check the update of f in bridging view
            rootAndChildInF.root = root.v;
            rootAndChildInF.child = child.v; // return the root after union
        } else { // the case that source and target have already been connected, return their root
            rootAndChildInF.root = rootOfSource.v;
            rootAndChildInF.child = EMPTY_ROOT;
        }
        return rootAndChildInF;
    }

    // check whether source and target are connected within the forward buffer
    @Deprecated
    public boolean intraConnected(int source, int target) {
        TreeNode sourceTreeNode = vertex2Node.get(source);
        if (sourceTreeNode == null)
            return false;
        TreeNode targetTreeNode = vertex2Node.get(target);
        if (targetTreeNode == null)
            return false;
        return find(sourceTreeNode) == find(targetTreeNode);
    }

    public boolean intraConnected(int workLoadIndex) {
        TreeNode sourceNode = sourceNodes[workLoadIndex];
        if (sourceNode == null) {
            sourceNode = vertex2Node.get(sources[workLoadIndex]);
            if (sourceNode == null)
                return false;
            sourceNodes[workLoadIndex] = sourceNode;
        }

        TreeNode targetNode = targetNodes[workLoadIndex];
        if (targetNode == null) {
            targetNode = vertex2Node.get(targets[workLoadIndex]);
            if (targetNode == null)
                return false;
            targetNodes[workLoadIndex] = targetNode;
        }
        return find(sourceNode) == find(targetNode);
    }

    // check whether source and target are connected within the forward buffer
    // if they are not connected, add their roots in pair rootsInF, where the first is the root of source and the second is the root of target

    @Deprecated
    public boolean intraConnected(int source, int target, RootPair rootsInF) {
        TreeNode rootSource = null, rootTarget = null;

        rootsInF.numOfEmpty = 0;

        TreeNode sourceTreeNode = vertex2Node.get(source);
        if (sourceTreeNode != null) {
            rootSource = find(sourceTreeNode);
            rootsInF.sourceRoot = rootSource.v;
        } else {
            rootsInF.sourceRoot = EMPTY_ROOT;
            rootsInF.numOfEmpty++;
        }

        TreeNode targetTreeNode = vertex2Node.get(target);
        if (targetTreeNode != null) {
            rootTarget = find(targetTreeNode);
            rootsInF.targetRoot = rootTarget.v;
        } else {
            rootsInF.targetRoot = EMPTY_ROOT;
            rootsInF.numOfEmpty++;
        }

        if (rootSource == null || rootTarget == null)
            return false;

        return rootSource == rootTarget;
    }

    void findSourceRootForIntraConnected(int workloadIndex, RootPair rootsInF) { // test only
        TreeNode sourceNode = sourceNodes[workloadIndex];
        if (sourceNode == null) {
            sourceNode = vertex2Node.get(sources[workloadIndex]);
            if (sourceNode != null) {
                sourceNodes[workloadIndex] = sourceNode;
                rootsInF.sourceRoot = find(sourceNode).v;
            }
        } else
            rootsInF.sourceRoot = find(sourceNode).v;
    }

    void findTargetRootForIntraConnected(int workloadIndex, RootPair rootsInF) { // test only
        TreeNode targetNode = targetNodes[workloadIndex];
        if (targetNode == null) {
            targetNode = vertex2Node.get(targets[workloadIndex]);
            if (targetNode != null) {
                targetNodes[workloadIndex] = targetNode;
                rootsInF.targetRoot = find(targetNode).v;
            }
        } else
            rootsInF.targetRoot = find(targetNode).v;
    }


    /**
     * Links the source vertex to the target vertex.
     * Returns the root after performing the union operation
     * If source or target was not included in the data structure, they will be first inserted before linking.
     */
    private TreeNode union(TreeNode sourceRoot, TreeNode targetRoot) {
        if (sourceRoot.sizeOfSubTree < targetRoot.sizeOfSubTree) { // source as child of target
            sourceRoot.parent = targetRoot; // set target as the parent of source
            targetRoot.sizeOfSubTree += sourceRoot.sizeOfSubTree; // update the size of the subtree rooted at target
            return targetRoot;
        } else {  // target as child of source
            targetRoot.parent = sourceRoot; // set source as the parent of target
            sourceRoot.sizeOfSubTree += targetRoot.sizeOfSubTree; // update the size of the subtree rooted at source
            return sourceRoot;
        }
    }

    /**
     * Compute the root of the tree node.
     * If the tree node does not have a parent, then return the tree node itself.
     */
//    private TreeNode find(TreeNode treeNode) {
//        TreeNode temp = treeNode;
//
//        while (temp.parent != null)
//            temp = temp.parent;
//
//        return temp;
//    }
    private TreeNode find(TreeNode treeNode) {
        TreeNode temp = treeNode;

        if (temp.parent == null) // treeNode is root
            return temp;

        temp = temp.parent; // temp is the parent of treeNode

        if (temp.parent == null)// the case of treeNode -> temp -> null, simply return temp
            return temp;

        treeNodeQueue.add(treeNode);// add treeNode in the queue
        treeNodeQueue.add(temp);// add temp in the queue

        temp = temp.parent;

        while (temp.parent != null) {
            treeNodeQueue.add(temp);
            temp = temp.parent;
        }

        TreeNode root = temp;

        treeNodeQueue.pollLast(); // remove the child of root;

        while (!treeNodeQueue.isEmpty()) {
            temp = treeNodeQueue.poll();
            temp.parent.sizeOfSubTree -= temp.sizeOfSubTree;
            temp.parent = root;
        }

        return root;
    }

    static class TreeNode {
        final int v;
        TreeNode parent;
        int sizeOfSubTree;

        public TreeNode(int v) {
            this.v = v;
            this.sizeOfSubTree = 1;
            this.parent = null;
        }
    }

}
