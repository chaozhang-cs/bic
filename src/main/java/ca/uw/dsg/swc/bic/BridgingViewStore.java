package ca.uw.dsg.swc.bic;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import static ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity.EMPTY_ROOT;

public class BridgingViewStore {

    // f2b and b2f store the same BipartiteEdges
    private final Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<BipartiteEdge>> f2B;
    private final Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<BipartiteEdge>> b2F;

    private final int chunkSize;

    public BridgingViewStore(int chunkSize) {
        this.b2F = new Int2ObjectOpenHashMap<>();
        this.f2B = new Int2ObjectOpenHashMap<>();
        this.chunkSize = chunkSize;
    }


    public void edgeInsertion(int vf, int vb, int low, int high) {
        // get the adjacency list of vf
        Int2ObjectOpenHashMap<BipartiteEdge> vfAdj = f2B.computeIfAbsent(vf, k -> new Int2ObjectOpenHashMap<>());
        BipartiteEdge bipartiteEdge = vfAdj.get(vb);

        if (bipartiteEdge == null) {// vf and vb are not adjacent
            bipartiteEdge = new BipartiteEdge(vb, vf, chunkSize);
            vfAdj.put(vb, bipartiteEdge);

            b2F.computeIfAbsent(vb, k -> new Int2ObjectOpenHashMap<>()).put(vf, bipartiteEdge);
        }
        bipartiteEdge.intervals.insert(low, high); // insert intervals
    }


    public void updateForwardV(int oldV, int newV) {
        if (f2B.containsKey(oldV)) { // oldV must be contained in the forward buffer
            Int2ObjectOpenHashMap<BipartiteEdge> oldVAdj = f2B.remove(oldV); // vertices and intervals adjacent to oldV

            if (f2B.containsKey(newV)) { // if newV already exists
                Int2ObjectOpenHashMap<BipartiteEdge> newVAdj = f2B.get(newV);
                for (Int2ObjectMap.Entry<BipartiteEdge> entry : oldVAdj.int2ObjectEntrySet()) { // iterating the adjacency list of oldV
                    int vb = entry.getIntKey();
                    BipartiteEdge bipartiteEdge = entry.getValue();

                    // change (vb, oldV) to be (vb, newV)
                    if (!newVAdj.containsKey(vb)) { // if newV is not adjacent to v that is adjacent to oldV, simple insertion
                        BipartiteEdge temp = new BipartiteEdge(vb, newV, chunkSize);
                        temp.intervals.insertAll(bipartiteEdge.intervals);
                        newVAdj.put(vb, temp);
                        Int2ObjectOpenHashMap<BipartiteEdge> vbAdj = b2F.get(vb);
                        vbAdj.remove(oldV);
                        vbAdj.put(newV, temp);


                    } else { // if newV is adjacent to v that is adjacent to oldV, inserting all the intervals
                        newVAdj.get(vb).intervals.insertAll(bipartiteEdge.intervals);

                        b2F.get(vb).remove(oldV);
                    }
                }
            } else {
                f2B.put(newV, oldVAdj); // insert newV and the bipartite edges
                for (Int2ObjectMap.Entry<BipartiteEdge> entry : oldVAdj.int2ObjectEntrySet()) {  // for each (vb, BipartiteEdge) that is adjacent to oldV
                    int vb = entry.getIntKey();
                    BipartiteEdge bipartiteEdge = entry.getValue();
                    bipartiteEdge.vf = newV; // update the corresponding vf to newV
                    Int2ObjectOpenHashMap<BipartiteEdge> vbAdj = b2F.get(vb);
                    vbAdj.remove(oldV);
                    vbAdj.put(newV, bipartiteEdge);
                }
            }
        }
    }

    boolean query(RootPair rootsInB, RootPair rootsInF, int index) {
        int sourceB = rootsInB.sourceRoot,
                sourceF = rootsInF.sourceRoot,
                targetB = rootsInB.targetRoot,
                targetF = rootsInF.targetRoot;
        boolean ret = false;

        // initialization of the bidirectional BFS on the bipartite graph
        IntOpenHashSet sourceVisitedF = new IntOpenHashSet(), sourceVisitedB = new IntOpenHashSet(), targetVisitedF = new IntOpenHashSet(), targetVisitedB = new IntOpenHashSet();
        BipartiteVertexQueue sourceQueue = new BipartiteVertexQueue(), targetQueue = new BipartiteVertexQueue();

        // set up visited vertices and queues
        if (sourceB != EMPTY_ROOT) {
            sourceVisitedB.add(sourceB);
            sourceQueue.enqueue(sourceB, BipartiteVertex.V_B);
        }
        if (sourceF != EMPTY_ROOT) {
            sourceVisitedF.add(sourceF);
            sourceQueue.enqueue(sourceF, BipartiteVertex.V_F);
        }
        if (targetB != EMPTY_ROOT) {
            targetVisitedB.add(targetB);
            targetQueue.enqueue(targetB, BipartiteVertex.V_B);
        }
        if (targetF != EMPTY_ROOT) {
            targetVisitedF.add(targetF);
            targetQueue.enqueue(targetF, BipartiteVertex.V_F);
        }


        while (!sourceQueue.isEmpty() && !targetQueue.isEmpty()) {
            // source start
            BipartiteVertex vsbv = sourceQueue.dequeue();
            int vs = vsbv.v;
            if (vsbv.isF == BipartiteVertex.V_F) { // vertex vs in F
                if (targetVisitedF.contains(vs)) { // searches from source and target visit the same vertex in F
                    ret = true;
                    break;
                }
                Int2ObjectOpenHashMap<BipartiteEdge> adjVs = f2B.get(vs);
                if (adjVs != null) {
                    for (Int2ObjectMap.Entry<BipartiteEdge> entry : adjVs.int2ObjectEntrySet()) { // iterating all the neighbours of v, which are in B
                        if (!sourceVisitedB.contains(entry.getIntKey()) && entry.getValue().intervals.containsKey(index)) { // not visited, and interval includes index
                            sourceVisitedB.add(entry.getIntKey());
                            sourceQueue.enqueue(entry.getIntKey(), BipartiteVertex.V_B);
                        }
                    }
                }

            } else { // vertex vs in B
                if (targetVisitedB.contains(vs)) {
                    ret = true;
                    break;
                }
                Int2ObjectOpenHashMap<BipartiteEdge> adjVs = b2F.get(vs);
                if (adjVs != null) {
                    for (Int2ObjectMap.Entry<BipartiteEdge> entry : adjVs.int2ObjectEntrySet()) {
                        if (!sourceVisitedF.contains(entry.getIntKey()) && entry.getValue().intervals.containsKey(index)) {
                            sourceVisitedF.add(entry.getIntKey());
                            sourceQueue.enqueue(entry.getIntKey(), BipartiteVertex.V_F);
                        }
                    }
                }
            }
            // source end
            // ***********
            // target start
            BipartiteVertex vtbv = targetQueue.dequeue();
            int vt = vtbv.v;
            if (vtbv.isF == BipartiteVertex.V_F) { // vertex vt in F
                if (sourceVisitedF.contains(vt)) {
                    ret = true;
                    break;
                }
                Int2ObjectOpenHashMap<BipartiteEdge> adjVt = f2B.get(vt);
                if (adjVt != null) {
                    for (Int2ObjectMap.Entry<BipartiteEdge> entry : adjVt.int2ObjectEntrySet()) {// iterating all the neighbours of v, which are in B
                        if (!targetVisitedB.contains(entry.getIntKey()) && entry.getValue().intervals.containsKey(index)) {
                            targetVisitedB.add(entry.getIntKey());
                            targetQueue.enqueue(entry.getIntKey(), BipartiteVertex.V_B);
                        }
                    }
                }

            } else { // vertex vt in B
                if (sourceVisitedB.contains(vt)) {
                    ret = true;
                    break;
                }
                Int2ObjectOpenHashMap<BipartiteEdge> adjVt = b2F.get(vt);
                if (adjVt != null) {
                    for (Int2ObjectMap.Entry<BipartiteEdge> entry : adjVt.int2ObjectEntrySet()) {
                        if (!targetVisitedF.contains(entry.getIntKey()) && entry.getValue().intervals.containsKey(index)) {
                            targetVisitedF.add(entry.getIntKey());
                            targetQueue.enqueue(entry.getIntKey(), BipartiteVertex.V_F);
                        }
                    }
                }
            }
        }
        return ret;
    }


    private static class BipartiteEdge {
        private final IntervalStore intervals;

        private int vb;
        private int vf;

        public BipartiteEdge(int vb, int vf, int size) {
            this.vb = vb;
            this.vf = vf;
            intervals = new IntervalStore(size);
        }
    }

    private static class BipartiteVertexQueue {
        private final IntArrayFIFOQueue vQueue;
        private final IntArrayFIFOQueue isVFQueue;

        private final BipartiteVertex ret;

        public BipartiteVertexQueue() {
            vQueue = new IntArrayFIFOQueue();
            isVFQueue = new IntArrayFIFOQueue();
            this.ret = new BipartiteVertex();
        }

        public void enqueue(int v, int isF) {
            vQueue.enqueue(v);
            isVFQueue.enqueue(isF);
        }

        public BipartiteVertex dequeue() {
            ret.v = vQueue.dequeueInt();
            ret.isF = isVFQueue.dequeueInt();
            return ret;
        }

        public boolean isEmpty() {
            return vQueue.isEmpty();
        }
    }

    private static class BipartiteVertex {
        private int v;
        private int isF;

        static final int V_F = 1, V_B = 0;
    }

}
