package ca.uw.dsg.swc.baselines.etr;

public class SplitTreaps<T> {
    private ImplicitTreapNode<T> firstSubtreeRoot;
    private ImplicitTreapNode<T> secondSubtreeRoot;

    public SplitTreaps(ImplicitTreapNode<T> first, ImplicitTreapNode<T> second) {
        this.firstSubtreeRoot = first;
        this.secondSubtreeRoot = second;
    }

    public ImplicitTreapNode<T> getFirstSubtreeRoot() {
        return firstSubtreeRoot;
    }

    public ImplicitTreapNode<T> getSecondSubtreeRoot() {
        return secondSubtreeRoot;
    }

    SplitTreaps<T> setFirstSubtreeRoot(ImplicitTreapNode<T> firstSubtreeRoot) {
        this.firstSubtreeRoot = firstSubtreeRoot;
        return this;
    }

    SplitTreaps<T> setSecondSubtreeRoot(ImplicitTreapNode<T> secondSubtreeRoot) {
        this.secondSubtreeRoot = secondSubtreeRoot;
        return this;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "firstSubtreeRoot=" + firstSubtreeRoot +
                ", secondSubtreeRoot=" + secondSubtreeRoot +
                '}';
    }
}