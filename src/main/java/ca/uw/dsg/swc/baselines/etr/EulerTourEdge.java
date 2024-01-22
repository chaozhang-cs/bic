package ca.uw.dsg.swc.baselines.etr;

public class EulerTourEdge {
    private final InternalVertex source, target;
    private final ImplicitTreapNode<EulerTourEdge> implicitTreapNode;

    public EulerTourEdge(InternalVertex source, InternalVertex target) {
        this.source = source;
        this.target = target;
        this.implicitTreapNode = new ImplicitTreapNode<>(this);
    }

    public InternalVertex getSource() {
        return source;
    }

    public InternalVertex getTarget() {
        return target;
    }

    public ImplicitTreapNode<EulerTourEdge> getImplicitTreapNode() {
        return implicitTreapNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EulerTourEdge that = (EulerTourEdge) o;

        if (!source.equals(that.source)) return false;
        return target.equals(that.target);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + target.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EulerTourEdge{" +
                "source=" + source +
                ", target=" + target +
                '}';
    }
}
