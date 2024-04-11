package ca.uw.dsg.swc.baselines;

import ca.uw.dsg.swc.AbstractSlidingWindowConnectivity;
import ca.uw.dsg.swc.StreamingEdge;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.openjdk.jol.info.GraphLayout;

import java.time.Duration;
import java.util.*;

public class FdcSlidingWindowConnectivity extends AbstractSlidingWindowConnectivity {

    private final FullyDynamicConnectivity fdc;

    private final Queue<StreamingEdge> window;

    @Deprecated
    public FdcSlidingWindowConnectivity(Duration range, Duration slide, FullyDynamicConnectivity fullyDynamicConnectivity) {
        super(range, slide);
        this.fdc = fullyDynamicConnectivity;
        this.window = new ArrayDeque<>();
    }

    public FdcSlidingWindowConnectivity(Duration range, Duration slide, List<IntIntPair> workload, FullyDynamicConnectivity fullyDynamicConnectivity) {
        super(range, slide, workload);
        this.fdc = fullyDynamicConnectivity;
        this.window = new ArrayDeque<>();
    }


    @Override
    public void insert(StreamingEdge streamingEdge) {
        window.add(streamingEdge);
        fdc.insertEdge(streamingEdge.source, streamingEdge.target);
    }

    @Override
    public void evict(long lessThan) {
        while (!window.isEmpty() && window.peek().timeStamp < lessThan) {
            StreamingEdge streamingEdge = window.poll();
            fdc.deleteEdge(streamingEdge.source, streamingEdge.target);
        }
    }

    @Override
    public boolean query(int source, int target) {
        return fdc.connected(source, target);
    }

    @Override
    public void query(List<IntIntPair> workload, List<List<Boolean>> outputStreams) {
        for (int i = 0, num = workload.size(); i < num; i++)
            outputStreams.get(i).add(query(workload.get(i).firstInt(), workload.get(i).secondInt()));
//        System.out.println("Number of edges in the window: " + window.size());
    }

    @Override
    public long memoryConsumption() {
        return GraphLayout.parseInstance(fdc).totalSize();
    }

}
