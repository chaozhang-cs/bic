package ca.uw.dsg.swc;

public class StreamingEdge {
    public final int source, target;
    public final long timeStamp;

    public StreamingEdge(int source, int target, long timeStamp) {
        this.source = source;
        this.target = target;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "StreamingEdge{" +
                "source=" + source +
                ", target=" + target +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
