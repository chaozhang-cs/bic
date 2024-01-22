package ca.uw.dsg.swc.bic;

import ca.uw.dsg.swc.AbstractSlidingWindowConnectivity;
import ca.uw.dsg.swc.StreamingEdge;
import it.unimi.dsi.fastutil.ints.IntIntPair;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class BidirectionalIncrementalConnectivity extends AbstractSlidingWindowConnectivity {
    private Chunk currentChunk;
    private final int chunkSize;
    private int windowIndexInChunk; // if this value is 1, then the corresponding query needs to use BackwardBuffer
    private long startOfEachChunk;
    private final long durationOfChunk;
    private BackwardForwardBufferPair bf;
    static final int EMPTY_ROOT = -1;
    static boolean[] base;

    @Deprecated
    public BidirectionalIncrementalConnectivity(Duration range, Duration slide, long firstTimeStamp) {
        super(range, slide);
        windowIndexInChunk = 0;

        chunkSize = (int) (super.range / super.slide);

        startOfEachChunk = firstTimeStamp;
        currentChunk = new Chunk(chunkSize, super.slide);
        currentChunk.setStartTime(startOfEachChunk);

        durationOfChunk = chunkSize * super.slide;

        bf = new BackwardForwardBufferPair(new BackwardBuffer(new Chunk(chunkSize, super.slide)), new ForwardBuffer());

        base = new boolean[chunkSize];
        Arrays.fill(base, true);
    }

    public BidirectionalIncrementalConnectivity(Duration range, Duration slide, long firstTimeStamp, List<IntIntPair> workloads) {
        super(range, slide, workloads);
        windowIndexInChunk = 0;

        chunkSize = (int) (super.range / super.slide);

        startOfEachChunk = firstTimeStamp;
        currentChunk = new Chunk(chunkSize, super.slide);
        currentChunk.setStartTime(startOfEachChunk);

        durationOfChunk = chunkSize * super.slide;

        bf = new BackwardForwardBufferPair(new BackwardBuffer(new Chunk(chunkSize, super.slide), workloads), new ForwardBuffer(workloads));

        base = new boolean[chunkSize];
        Arrays.fill(base, true);
    }


    // the insert operation creates new forward and backward buffers and adds them into queues
    // the query operation will take forward and backward buffer from the queues and set up the current forward and backward buffers
    @Override
    public void insert(StreamingEdge streamingEdge) {
        // check if the current chunk is full, if so get the next chunk and insert into the current chunk
        // insert into the current chunk

        if (!currentChunk.insert(streamingEdge)) { // current chunk is full
            // add the vertex set of the complete chunk into the backward buffer, which help prune vertices that are not inter-vertices during inserting into TaskQueue
            BackwardBuffer backwardBuffer = new BackwardBuffer(currentChunk, workload);
//            long start = System.nanoTime();
            backwardBuffer.compute();
//            System.out.println("Backward buffer computation time: " + (System.nanoTime() - start));

            bf = new BackwardForwardBufferPair(backwardBuffer, new ForwardBuffer(workload));

            // get a new chunk
            startOfEachChunk += durationOfChunk;
            currentChunk = new Chunk(chunkSize, super.slide);
            currentChunk.setStartTime(startOfEachChunk);

            // insert edge into the new chunk
            currentChunk.insert(streamingEdge);
        }
        bf.insert(streamingEdge, windowIndexInChunk);
    }

    @Override
    public void evict(long lessThan) {
    }


    @Override
    public boolean query(int source, int target) { // retrieve a computed backward buffer from the BackwardBuffer queue

        boolean ret;
        if (windowIndexInChunk == 0) {
            // compute query result using forward buffer
            ret = bf.queryWithF(source, target);
        } else
            ret = bf.queryWithBF(windowIndexInChunk, source, target);

        if (++windowIndexInChunk == chunkSize)
            windowIndexInChunk = 0;
        return ret;
    }

    @Override
    public void query(List<IntIntPair> queries, List<List<Boolean>> outputStreams) {
        if (windowIndexInChunk == 0) {
            // compute query result using forward buffer
            for (int i = 0, num = queries.size(); i < num; i++)
                outputStreams.get(i).add(bf.queryWithF(i));
        } else
            for (int i = 0, num = queries.size(); i < num; i++)
                outputStreams.get(i).add(bf.queryWithBF(windowIndexInChunk, i));
        if (++windowIndexInChunk == chunkSize)
            windowIndexInChunk = 0;
    }

    private static class BackwardForwardBufferPair {
        private final BackwardBuffer b;
        private final ForwardBuffer f;

        private final RootPair rootsInF, rootsInB;

        public BackwardForwardBufferPair(BackwardBuffer backwardBuffer, ForwardBuffer forwardBuffer) {
            // bridging view is embedded in backward buffer
            // as the updates can be performed in a more efficient way by directly accessing augmented tree node in backward buffer
            b = backwardBuffer;
            f = forwardBuffer;
            rootsInF = new RootPair(); // for the case of inter-buffer checking
            rootsInB = new RootPair(); // for the case of inter-buffer checking
        }

        void insert(StreamingEdge streamingEdge, int indexInB) {
            // 1. insert into f
            // 2. update bridging view
            //      2.1 update forward vertex
            //      2.2 insert new edges into bridging view
            RootAndChild rootAndChildInF = f.insertEdgeAndGetRoots(streamingEdge.source, streamingEdge.target);


            if (indexInB == 0)
                return;

            // the case that source and target were not connected before inserting the streaming edge
            int rootInF = rootAndChildInF.root; // the root after inserting the streaming edge
            int oldRootInF = rootAndChildInF.child; // the child of the new root, such that the child was a root before inserting the streaming edge

            if (oldRootInF != EMPTY_ROOT) // the case that update f in the bipartite graph
                b.updateNewRootInF(oldRootInF, rootInF);

            b.insertBGEdgeWithInterVertex(streamingEdge.source, rootInF, indexInB); // the case of insert caused by inter-vertex
            b.insertBGEdgeWithInterVertex(streamingEdge.target, rootInF, indexInB); // the case of insert caused by inter-vertex
        }

        boolean queryWithF(int workLoadIndex) {
            return f.intraConnected(workLoadIndex);
        }

        boolean queryWithBF(int indexInB, int workloadIndex) {
            rootsInF.sourceRoot = EMPTY_ROOT;
            rootsInF.targetRoot = EMPTY_ROOT;
            rootsInB.sourceRoot = EMPTY_ROOT;
            rootsInB.targetRoot = EMPTY_ROOT;

            // check source: start
            f.findSourceRootForIntraConnected(workloadIndex, rootsInF); // compute source root in F
            if (rootsInF.sourceRoot == EMPTY_ROOT) {// sourceRoot in F is empty
                b.findSourceRootForIntraConnected(workloadIndex, indexInB, rootsInB); // compute source root in B
                if (rootsInB.sourceRoot == EMPTY_ROOT) // source root in B is empty
                    return false; // source does not exit
            }
            // check source: end

            // check target: start
            f.findTargetRootForIntraConnected(workloadIndex, rootsInF);
            if (rootsInF.targetRoot == EMPTY_ROOT) {// targetRoot in F is empty
                b.findTargetRootForIntraConnected(workloadIndex, indexInB, rootsInB);
                if (rootsInB.targetRoot == EMPTY_ROOT)
                    return false;
            }
            // check target: end

            if (rootsInF.sourceRoot != EMPTY_ROOT && rootsInF.sourceRoot == rootsInF.targetRoot)// intra connected in F
                return true;

            if (rootsInF.sourceRoot != EMPTY_ROOT) // source in b has not been computed
                b.findSourceRootForIntraConnected(workloadIndex, indexInB, rootsInB);
            if (rootsInF.targetRoot != EMPTY_ROOT)
                b.findTargetRootForIntraConnected(workloadIndex, indexInB, rootsInB);

            if (rootsInB.sourceRoot != EMPTY_ROOT && rootsInB.sourceRoot == rootsInB.targetRoot)// intra connected in B
                return true;

            return b.bridgingViewStore.query(rootsInB, rootsInF, indexInB);
        }

        @Deprecated
        boolean queryWithF(int source, int target) {
            return f.intraConnected(source, target);
        }

        @Deprecated
        boolean queryWithBF(int indexInB, int source, int target) {
            if (f.intraConnected(source, target, rootsInF))
                return true;
            if (b.intraConnected(source, target, indexInB, rootsInB))
                return true;

            if (rootsInF.numOfEmpty + rootsInB.numOfEmpty >= 3) // three roots are empty, then return false
                return false;
            if (rootsInF.sourceRoot == EMPTY_ROOT && rootsInB.sourceRoot == EMPTY_ROOT ||
                    rootsInF.targetRoot == EMPTY_ROOT && rootsInB.targetRoot == EMPTY_ROOT)
                return false;

            return b.bridgingViewStore.query(rootsInB, rootsInF, indexInB);
        }
    }
}
