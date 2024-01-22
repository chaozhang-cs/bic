package ca.uw.dsg.swc.bic;

import ca.uw.dsg.swc.StreamingEdge;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIntPair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity.EMPTY_ROOT;

public class BackwardBuffer {
    final Int2ObjectOpenHashMap<AugmentedTreeNode> vertex2AugTreeNode; // map vertex to its node in union-find tree
    final BridgingViewStore bridgingViewStore;

    final AtomicBoolean isDone;
    private final Chunk chunk;

    private int[] sources, targets;
    private AugmentedTreeNode[] sourceNodes, targetNodes;

    public BackwardBuffer(Chunk chunk) {
        this.chunk = chunk;
        this.vertex2AugTreeNode = new Int2ObjectOpenHashMap<>();
        this.bridgingViewStore = new BridgingViewStore(chunk.getChunkSize());
        this.isDone = new AtomicBoolean(false);
    }

    public BackwardBuffer(Chunk chunk, List<IntIntPair> workload) {
        this.chunk = chunk;
        this.vertex2AugTreeNode = new Int2ObjectOpenHashMap<>();
        this.bridgingViewStore = new BridgingViewStore(chunk.getChunkSize());
        this.isDone = new AtomicBoolean(false);
        int len = workload.size();
        sources = new int[len];
        targets = new int[len];
        sourceNodes = new AugmentedTreeNode[len];
        targetNodes = new AugmentedTreeNode[len];
        int i = 0;
        for (IntIntPair st : workload) {
            sources[i] = st.firstInt();
            targets[i++] = st.secondInt();
        }
    }

    @Deprecated
    public boolean intraConnected(int source, int target, int index, RootPair rootsInB) {
        AugmentedTreeNode rootSource = null, rootTarget = null;

        AugmentedTreeNode sourceAugmentedTreeNode = vertex2AugTreeNode.get(source);

        rootsInB.numOfEmpty = 0;

        if (sourceAugmentedTreeNode != null && index <= sourceAugmentedTreeNode.vertexLabel) {
            rootSource = find(sourceAugmentedTreeNode, index);
            rootsInB.sourceRoot = rootSource.v;
        } else {
            rootsInB.sourceRoot = EMPTY_ROOT;
            rootsInB.numOfEmpty++;
        }

        AugmentedTreeNode targetAugmentedTreeNode = vertex2AugTreeNode.get(target);

        if (targetAugmentedTreeNode != null && index <= targetAugmentedTreeNode.vertexLabel) {
            rootTarget = find(targetAugmentedTreeNode, index);
            rootsInB.targetRoot = rootTarget.v;
        } else {
            rootsInB.targetRoot = EMPTY_ROOT;
            rootsInB.numOfEmpty++;
        }

        if (rootSource == null || rootTarget == null)
            return false;
        return rootSource == rootTarget;
    }

    @Deprecated
    public boolean intraConnected(int workloadIndex, int index, RootPair rootsInB) {
        rootsInB.numOfEmpty = 0;

        AugmentedTreeNode sourceAugmentedTreeNode = sourceNodes[workloadIndex];
        if (sourceAugmentedTreeNode != null && index <= sourceAugmentedTreeNode.vertexLabel) {
            rootsInB.sourceRoot = find(sourceAugmentedTreeNode, index).v;
        } else {
            rootsInB.sourceRoot = EMPTY_ROOT;
            rootsInB.numOfEmpty++;
        }

        AugmentedTreeNode targetAugmentedTreeNode = targetNodes[workloadIndex];
        if (targetAugmentedTreeNode != null && index <= targetAugmentedTreeNode.vertexLabel) {
            rootsInB.targetRoot = find(targetAugmentedTreeNode, index).v;
        } else {
            rootsInB.targetRoot = EMPTY_ROOT;
            rootsInB.numOfEmpty++;
        }

        if (rootsInB.sourceRoot == EMPTY_ROOT || rootsInB.targetRoot == EMPTY_ROOT)
            return false;
        return rootsInB.sourceRoot == rootsInB.targetRoot;
    }


    void findSourceRootForIntraConnected(int workloadIndex, int index, RootPair rootsInB) {
        AugmentedTreeNode sourceAugmentedTreeNode = sourceNodes[workloadIndex];
        if (sourceAugmentedTreeNode != null && index <= sourceAugmentedTreeNode.vertexLabel)
            rootsInB.sourceRoot = find(sourceAugmentedTreeNode, index).v;
    }


    void findTargetRootForIntraConnected(int workloadIndex, int index, RootPair rootsInB) {
        AugmentedTreeNode targetAugmentedTreeNode = targetNodes[workloadIndex];
        if (targetAugmentedTreeNode != null && index <= targetAugmentedTreeNode.vertexLabel)
            rootsInB.targetRoot = find(targetAugmentedTreeNode, index).v;
    }

    private void insertEdge(int source, int target, int index) {
        AugmentedTreeNode sourceAugmentedTreeNode = vertex2AugTreeNode.computeIfAbsent(source, k -> new AugmentedTreeNode(source, index));
        AugmentedTreeNode targetAugmentedTreeNode = vertex2AugTreeNode.computeIfAbsent(target, k -> new AugmentedTreeNode(target, index));

        AugmentedTreeNode rootOfSource = find(sourceAugmentedTreeNode);
        AugmentedTreeNode rootOfTarget = find(targetAugmentedTreeNode);

        if (rootOfSource != rootOfTarget)
            // let the smaller one be the child of the larger one
            if (rootOfSource.sizeOfSubTree < rootOfTarget.sizeOfSubTree) // source as child of target, i.e., target is the root
                union(rootOfSource, rootOfTarget, index);
            else  // target as child of source
                union(rootOfTarget, rootOfSource, index);
    }

    private void union(AugmentedTreeNode child, AugmentedTreeNode parent, int index) {
        child.parent = parent; // set target as the parent of source
        parent.sizeOfSubTree += child.sizeOfSubTree; // update the size of the subtree rooted at target

        child.labelOfEdgeToParent = index; // label the edge from source to target with index

        if (!parent.hasInterval) {
            // if the root has not labeled with an interval, then label the root with an interval to indicate that when it is a root
            // notice that, the interval can be changed later if the vertex becomes a child of another vertex
            parent.hasInterval = true;
            parent.startInterval = 1; // assuming it is a root from the first snapshot index, i.e., 1;
            parent.endInterval = index;
        }

        // update the interval of the child
        // this only applies to the case that the child was labeled with an interval, and now the interval needs to be changed
        if (child.hasInterval) {
            if (child.endInterval == index) { // the child at chunk[index] is not eventually a root, such that remove the interval of the child
                child.hasInterval = false;
                child.startInterval = AugmentedTreeNode.DEFAULT_VALUE;
                child.endInterval = AugmentedTreeNode.DEFAULT_VALUE;
            } else {
                // endInterval cannot be smaller than the current index, because union is performed during backward computation
                // such that it is the case endInterval > index
                child.startInterval = index + 1;
            }
        }
    }

    private AugmentedTreeNode find(AugmentedTreeNode AugmentedTreeNode) {
        AugmentedTreeNode temp = AugmentedTreeNode;
        while (temp.parent != null)
            temp = temp.parent;
        return temp;
    }

    private AugmentedTreeNode find(AugmentedTreeNode AugmentedTreeNode, int index) {
        AugmentedTreeNode temp = AugmentedTreeNode;
        while (temp.parent != null && temp.labelOfEdgeToParent >= index) // only edges labeled with a larger index are visited, i.e., the snapshot isolation approach
            temp = temp.parent;
        return temp;
    }


    void insertBGEdgeWithInterVertex(int interV, int rootOfInterVInF, int indexInB) {
        AugmentedTreeNode augmentedTreeNode = vertex2AugTreeNode.get(interV);
        // vertexLabel l indicates that v is inserted at b[l], such that v can exist from b[1] to b[l]
        // if the snapshot index is smaller than l, then v exists in the backward buffer
        if (augmentedTreeNode != null && indexInB <= augmentedTreeNode.vertexLabel)
            insertBGEdge(interV, rootOfInterVInF, indexInB); //
    }

    void updateNewRootInF(int oldV, int newV) {
        bridgingViewStore.updateForwardV(oldV, newV);
    }


    // intervals are stored as a list of [start, end]
    // each root is associated with an interval
    // interV must be an inter-vertex
    private void insertBGEdge(int interV, int rootOfInterVInF, int indexInB) {
        AugmentedTreeNode temp = vertex2AugTreeNode.get(interV);
        final int vertexLabel = temp.vertexLabel; // vertexLabel indicates the largest snapshot index of the backward buffer that contains the vertex

        if (temp.parent == null) { // temp is the root, then temp must has an interval
            bridgingViewStore.edgeInsertion(rootOfInterVInF, temp.v, indexInB, vertexLabel);
            return;
        }

        int nextEnd = vertexLabel;
        if (temp.hasInterval && temp.startInterval <= vertexLabel) { // process temp
            bridgingViewStore.edgeInsertion(rootOfInterVInF, temp.v, temp.startInterval, nextEnd);
            nextEnd = temp.startInterval - 1;
        }

        while (temp.parent != null && indexInB <= temp.labelOfEdgeToParent) {// process temp's predecessors
            temp = temp.parent;
            if (temp.hasInterval && temp.startInterval <= vertexLabel) {
                // vertex temp is now the first vertex labeled with interval [start, end] such that start <= vertexLabel
                if (temp.parent == null) { // if temp is currently the root in the tree
                    bridgingViewStore.edgeInsertion(rootOfInterVInF, temp.v, indexInB, nextEnd);
                    return;
                } else { // temp is not the root
                    bridgingViewStore.edgeInsertion(rootOfInterVInF, temp.v, temp.startInterval, nextEnd);
                    nextEnd = temp.startInterval - 1;
                }
            }
        }
    }


    public void compute() { // backward computation
        ArrayList<ArrayList<StreamingEdge>> edgesInChunk = chunk.getData();
        int chunkSize = edgesInChunk.size();
        for (int i = chunkSize - 1; i > 0; i--) { // from the last to the first, chunk[0] is not needed
            List<StreamingEdge> element = edgesInChunk.get(i);
            for (int j = element.size() - 1; j > -1; j--) { // from the last to the first
                StreamingEdge se = element.get(j); // retrieve streaming edges in the chunk
                insertEdge(se.source, se.target, i); // insert with index in B
            }
        }
        setupWorkloadNodes();
    }

    private void setupWorkloadNodes() {
        for (int i = 0, len = sources.length; i < len; i++) {
            sourceNodes[i] = vertex2AugTreeNode.get(sources[i]);
            targetNodes[i] = vertex2AugTreeNode.get(targets[i]);
        }
    }

    static class AugmentedTreeNode {
        static final int DEFAULT_VALUE = -1;
        int v;
        AugmentedTreeNode parent; // parent
        int sizeOfSubTree; // tree size
        int labelOfEdgeToParent; // the label of the edge to its parent
        int vertexLabel; // vertex label
        int startInterval, endInterval; // vertex interval
        boolean hasInterval;

        public AugmentedTreeNode(int v, int vertexLabel) {
            this.v = v;
            this.sizeOfSubTree = 1;
            this.parent = null;
            this.vertexLabel = vertexLabel;
            this.hasInterval = false;

            this.labelOfEdgeToParent = DEFAULT_VALUE;
            this.startInterval = DEFAULT_VALUE;
            this.endInterval = DEFAULT_VALUE;
        }
    }
}
