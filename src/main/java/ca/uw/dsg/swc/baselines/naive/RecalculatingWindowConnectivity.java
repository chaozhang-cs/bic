package ca.uw.dsg.swc.baselines.naive;

import ca.uw.dsg.swc.AbstractSlidingWindowConnectivity;
import ca.uw.dsg.swc.StreamingEdge;
import ca.uw.dsg.swc.baselines.etr.SpanningTree;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.openjdk.jol.info.GraphLayout;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class RecalculatingWindowConnectivity extends AbstractSlidingWindowConnectivity {
    private final Queue<StreamingEdge> window;
    private SpanningTree.IncrementalConnectivity ufts;

    public RecalculatingWindowConnectivity(Duration range, Duration slide) {
        super(range, slide);
        this.window = new ArrayDeque<>();
    }

    public RecalculatingWindowConnectivity(Duration range, Duration slide, List<IntIntPair> workload) {
        super(range, slide, workload);
        this.window = new ArrayDeque<>();
    }

    @Override
    public void insert(StreamingEdge streamingEdge) {
        window.add(streamingEdge);
    }

    @Override
    public void evict(long lessThan) {
        while (!window.isEmpty() && window.peek().timeStamp < lessThan)
            window.poll();
    }

    @Override
    public boolean query(int source, int target) {
        buildUfts();
        return this.ufts.connected(source, target);
    }

    @Override
    public void query(List<IntIntPair> queries, List<List<Boolean>> outputStreams) {
        buildUfts();
        for (int i = 0, num = queries.size(); i < num; i++) {
            IntIntPair intIntPair = queries.get(i);
            outputStreams.get(i).add(this.ufts.connected(intIntPair.firstInt(), intIntPair.secondInt()));
        }
    }

    private void buildUfts(){
        SpanningTree.IncrementalConnectivity incrementalConnectivity = new SpanningTree.IncrementalConnectivity();
        for (StreamingEdge e : window)
            incrementalConnectivity.union(e.source, e.target);
        this.ufts = incrementalConnectivity;
    }

    @Override
    public long memoryConsumption() {
        return GraphLayout.parseInstance(ufts).totalSize();
    }
}
