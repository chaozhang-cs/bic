package ca.uw.dsg.swc.baselines.etr;

import java.util.Random;

public class ImplicitTreapNode<T> {
    private final long priority;
    private ImplicitTreapNode<T> leftChild;
    private ImplicitTreapNode<T> rightChild;
    private ImplicitTreapNode<T> parent;
    private int size;
    private final T data;
    private static final Random RANDOM = new Random();

    // This implementation does not override the equals() method.

    public ImplicitTreapNode(T data) {
        this.priority = RANDOM.nextLong();
        this.data = data;
        this.leftChild = null;
        this.rightChild = null;
        this.parent = null;
        size = 1;
    }

    long getPriority() {
        return priority;
    }

    public ImplicitTreapNode<T> getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(ImplicitTreapNode<T> leftChild) {
        this.leftChild = leftChild;
        if (this.leftChild != null) this.leftChild.parent = this;
        updateSize();
    }

    public ImplicitTreapNode<T> getRightChild() {
        return rightChild;
    }

    public void setRightChild(ImplicitTreapNode<T> rightChild) {
        this.rightChild = rightChild;
        if (this.rightChild != null) this.rightChild.parent = this;
        updateSize();
    }

    void setNullParent() {
        if (this.parent != null) {
            ImplicitTreapNode<T> temp = this.parent;
            this.parent = null;
            if (temp.leftChild == this)
                temp.leftChild = null;
            else if (temp.rightChild == this)
                temp.rightChild = null;
            temp.updateSize();
        }
    }

    public ImplicitTreapNode<T> getParent() {
        return parent;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public ImplicitTreapNode<T> getRoot() {
        ImplicitTreapNode<T> ret = this;
        while (!ret.isRoot()) {
            ret = ret.parent;
        }
        return ret;
    }

    public int getSize() {
        return size;
    }

    public T getData() {
        return data;
    }

    @Override
    public String toString() {
        return "ImplicitTreapNode{" + "priority=" + priority + ", size=" + size + ", data=" + data + '}';
    }

    private void updateSize() {
        int temp = leftChild == null ? 0 : leftChild.size;
        temp += rightChild == null ? 0 : rightChild.size;
        temp += 1;
        size = temp;
    }
}