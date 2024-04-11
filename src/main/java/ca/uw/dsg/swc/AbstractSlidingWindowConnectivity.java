package ca.uw.dsg.swc;

import it.unimi.dsi.fastutil.ints.IntIntPair;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public abstract class AbstractSlidingWindowConnectivity {
    protected final long range, slide;
    protected final boolean isRangeMultipleOfSlide;

    protected List<IntIntPair> workload;

    public AbstractSlidingWindowConnectivity(Duration range, Duration slide) {
        this.range = range.toMillis();
        this.slide = slide.toMillis();
        this.isRangeMultipleOfSlide = (range.toMillis() % slide.toMillis() == 0);
    }

    public AbstractSlidingWindowConnectivity(Duration range, Duration slide, List<IntIntPair> workload) {
        this.range = range.toMillis();
        this.slide = slide.toMillis();
        this.isRangeMultipleOfSlide = (range.toMillis() % slide.toMillis() == 0);
        this.workload = workload;
    }

    // assuming timestamps of streaming edges are contiguous, i.e., the difference between every two adjacent time stamps is less than a slide interval
    // the content of the every window instance: [t_start, t_end)
    public void computeSlidingWindowConnectivity(Collection<StreamingEdge> inputStream, List<List<Boolean>> outputStreams) {
        if (inputStream.isEmpty())
            return;

        final int num = outputStreams.size();
        if (workload.size() != num)
            return;

        Iterator<StreamingEdge> streamingEdgeIterator = inputStream.iterator();
        StreamingEdge streamingEdge = streamingEdgeIterator.next();
        long startOfCurrentWindow = streamingEdge.timeStamp;
        insert(streamingEdge);

        while (streamingEdgeIterator.hasNext()) { // make the first window instance full
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow < range)
                insert(streamingEdge);
            else
                break;
        }

        query(workload, outputStreams);
        startOfCurrentWindow += slide;
        evict(startOfCurrentWindow); // first evict
        insert(streamingEdge);

        while (streamingEdgeIterator.hasNext()) {
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow >= range) { // compute query result
                query(workload, outputStreams);
                startOfCurrentWindow += slide;
                evict(startOfCurrentWindow);
            }
            insert(streamingEdge);
        }
    }

    // test only for latency experiments
    public void computeSlidingWindowConnectivity(Collection<StreamingEdge> inputStream, List<List<Boolean>> outputStreams, List<Long> latencyPerEdge) {
        if (inputStream.isEmpty())
            return;

        final int num = outputStreams.size();
        if (workload.size() != num)
            return;

        Iterator<StreamingEdge> streamingEdgeIterator = inputStream.iterator();
        StreamingEdge streamingEdge = streamingEdgeIterator.next();

        long startOfCurrentWindow = streamingEdge.timeStamp;
        insert(streamingEdge);

        while (streamingEdgeIterator.hasNext()) { // make the first window instance full
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow < range)
                insert(streamingEdge);
            else
                break;
        }

        query(workload, outputStreams);
        startOfCurrentWindow += slide;
        evict(startOfCurrentWindow); // first evict
        insert(streamingEdge);

        boolean isEnd = false;
        long start = 0, end = 0;

        while (streamingEdgeIterator.hasNext()) {
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow >= range) { // compute query result
                isEnd = true;

                start = System.nanoTime();
                query(workload, outputStreams);
                startOfCurrentWindow += slide;
                evict(startOfCurrentWindow);
            }
            insert(streamingEdge);

            end = System.nanoTime();

            if (isEnd) {
                latencyPerEdge.add(end - start);
                isEnd = false;
            }
        }
    }

    public void computeSlidingWindowConnectivity(Queue<StreamingEdge> inputStream, List<Boolean> outputStream, int source, int target) {
        if (inputStream.isEmpty())
            return;

        long startOfCurrentWindow = inputStream.peek().timeStamp;

        while (!inputStream.isEmpty() && (inputStream.peek().timeStamp - startOfCurrentWindow) < range) // make the first window instance full
            insert(inputStream.poll());

        outputStream.add(query(source, target));    // first window instance
        startOfCurrentWindow += slide;
        evict(startOfCurrentWindow); // first evict

        while (!inputStream.isEmpty()) {
            StreamingEdge streamingEdge = inputStream.poll();
            if (streamingEdge.timeStamp - startOfCurrentWindow >= range) { // compute query result
                outputStream.add(query(source, target));
                startOfCurrentWindow += slide;
                evict(startOfCurrentWindow);
            }
            insert(streamingEdge);
        }
    }

    public void computeQueriesAndGetMemoryConsumption(Collection<StreamingEdge> inputStream, List<List<Boolean>> outputStreams, List<Long> memoryConsumptionPerWindow) {
        if (inputStream.isEmpty())
            return;

        final int num = outputStreams.size();
        if (workload.size() != num)
            return;

        Iterator<StreamingEdge> streamingEdgeIterator = inputStream.iterator();
        StreamingEdge streamingEdge = streamingEdgeIterator.next();
        long startOfCurrentWindow = streamingEdge.timeStamp;
        insert(streamingEdge);

        while (streamingEdgeIterator.hasNext()) { // make the first window instance full
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow < range)
                insert(streamingEdge);
            else
                break;
        }

        query(workload, outputStreams);
        startOfCurrentWindow += slide;
        evict(startOfCurrentWindow); // first evict
        insert(streamingEdge);


        while (streamingEdgeIterator.hasNext()) {
            streamingEdge = streamingEdgeIterator.next();
            if (streamingEdge.timeStamp - startOfCurrentWindow >= range) { // compute query result
                query(workload, outputStreams);
                startOfCurrentWindow += slide;

                memoryConsumptionPerWindow.add(memoryConsumption()); // capturing the memory used

                evict(startOfCurrentWindow);
            }
            insert(streamingEdge);
        }
    }

    public abstract void insert(StreamingEdge StreamingEdge);

    // evict all the streaming edges, whose timestamp are less than the lessThan time
    public abstract void evict(long lessThan);

    public abstract boolean query(int source, int target);

    public abstract void query(List<IntIntPair> queries, List<List<Boolean>> outputStreams);

    public abstract long memoryConsumption();
}
