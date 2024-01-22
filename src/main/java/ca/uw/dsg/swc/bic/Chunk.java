package ca.uw.dsg.swc.bic;

import ca.uw.dsg.swc.StreamingEdge;

import java.util.ArrayList;

public class Chunk {
    public int getChunkSize() {
        return chunkSize;
    }

    private final int chunkSize; // r/s
    private int currentIndex; // used for inserting edges
    private final long intervalOfEachElement; // the slide parameter
    private long endOfCurrentElement;

    // the element in chunk are retried by using the remove method
    // such that chunk can be reused
    private final ArrayList<ArrayList<StreamingEdge>> data;
    private ArrayList<StreamingEdge> currentElement;

    public Chunk(int chunkSize, long intervalOfEachElement) {
        this.chunkSize = chunkSize;
        this.intervalOfEachElement = intervalOfEachElement;

        this.data = new ArrayList<>(chunkSize);
        for (int i = 0; i < chunkSize; i++)
            data.add(new ArrayList<>());
        currentIndex = 0;
        currentElement = data.get(currentIndex);
    }

    public boolean insert(StreamingEdge StreamingEdge) {
        if (StreamingEdge.timeStamp > endOfCurrentElement) { // the current element in the chunk is full, and add the streaming edge into the next element in the chunk
            // assuming the timestamp is in the next element
            // the case that there are gaps that are larger than the interval of each element is not considered
            if (++currentIndex < chunkSize) { // if the current chunk is not full
                currentElement = data.get(currentIndex); // get the next element
                endOfCurrentElement += intervalOfEachElement; // update the endTimeStamp of the current element
            } else   // the current chunk is full
                return false;
        }
        // adding streaming edge into the current element in the chunk
        currentElement.add(StreamingEdge);
        return true;
    }

    public void setStartTime(long startTime) {
        endOfCurrentElement = startTime + intervalOfEachElement - 1;
    }

    public ArrayList<ArrayList<StreamingEdge>> getData() {
        return data;
    }
}
