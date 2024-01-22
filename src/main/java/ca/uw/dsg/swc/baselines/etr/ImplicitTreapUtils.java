package ca.uw.dsg.swc.baselines.etr;


import java.util.List;
import java.util.Objects;

public class ImplicitTreapUtils {
    /**
     * Compute the index of the given implicit node in the corresponding implicit treap.
     * An implicit treap can be seen as a dynamic array starting from index 1.
     *
     * @param implicitTreapNode the implicit node that requires to compute the index
     * @return the index of the given implicitTreapNode
     * @throws NullPointerException throws NullPointerException if the given implicitTreapNode is null
     */
    public static <T> int indexOf(ImplicitTreapNode<T> implicitTreapNode) {
        Objects.requireNonNull(implicitTreapNode);
        int index = sizeOfTreapRootedAt(implicitTreapNode.getLeftChild()) + 1;
        ImplicitTreapNode<T> temp = implicitTreapNode;
        while (!temp.isRoot()) {
            if (temp.getParent().getRightChild() == temp)
                index += sizeOfTreapRootedAt(temp.getParent().getLeftChild()) + 1;
            temp = temp.getParent();
        }
        return index;
    }

    private static <T> int sizeOfTreapRootedAt(ImplicitTreapNode<T> implicitTreapNode) {
        if (implicitTreapNode == null)
            return 0;
        return implicitTreapNode.getSize();
    }

    /**
     * Compute the size of the tree containing the given implicit treap node.
     */
    public static <T> int sizeOfTreap(ImplicitTreapNode<T> implicitTreapNode) {
        return implicitTreapNode.getRoot().getSize();
    }

    private static <T> SplitTreaps<T> split(ImplicitTreapNode<T> root, int indexToBeSplit) {
        if (root == null)
            return new SplitTreaps<>(null, null);

        root.setNullParent();

        SplitTreaps<T> ret;
        int indexOfRoot = sizeOfTreapRootedAt(root.getLeftChild()) + 1;

        if (indexOfRoot <= indexToBeSplit) {
            ret = split(root.getRightChild(), indexToBeSplit - indexOfRoot);
            root.setRightChild(ret.getFirstSubtreeRoot());
            ret.setFirstSubtreeRoot(root);
        } else {
            ret = split(root.getLeftChild(), indexToBeSplit);
            root.setLeftChild(ret.getSecondSubtreeRoot());
            ret.setSecondSubtreeRoot(root);
        }
        return ret;
    }

    /**
     * Split the given implicit treap at the given index.
     *
     * @param root  the root of the given implicit treap
     * @param index the index where the given implicit treap needs to be split
     * @return a pair of implicit treaps (T0, T1), where the index of every node in T0 is less than or equal to the given index, and T1 greater than the given index.
     */
    public static <T> SplitTreaps<T> splitAt(ImplicitTreapNode<T> root, int index) {
        Objects.requireNonNull(root);
        if (!root.isRoot())
            throw new IllegalArgumentException("The input implicit node must be a root");

        if (root.getSize() <= index)
            return new SplitTreaps<>(root, null);

        return split(root, index);
    }

    /**
     * Split the implicit treap that contains the given implicit treap node at the given implicit treap node.
     *
     * @param implicitTreapNode the implicit treap node that requires to be split at
     * @return a pair of implicit treaps (T0, T1), where every treap node in T0 is less than or equal to the given node, and T1 greater than the given node.
     */
    public static <T> SplitTreaps<T> split(ImplicitTreapNode<T> implicitTreapNode) {
        if (implicitTreapNode == null)
            return null;
        return split(implicitTreapNode.getRoot(), indexOf(implicitTreapNode));
    }

    /**
     * Split the implicit treap that contains the given implicit treap node at the given implicit treap node.
     * If isLessThanOrEqualTo is true, then this method is equivalent to the method split(ImplicitTreapNode<T> implicitTreapNode).
     *
     * @param implicitTreapNode   the implicit treap node that requires to be split at
     * @param isLessThanOrEqualTo the flag indicate whether the implicit treap nodes is in the first or the second split sub-treap.
     * @return a pair of implicit treaps (T0, T1). If isLessThanOrEqualTo is false, where every treap node in T0 is less than the given node, and T1 equal to or greater than the given node.
     */
    public static <T> SplitTreaps<T> split(ImplicitTreapNode<T> implicitTreapNode, boolean isLessThanOrEqualTo) {
        SplitTreaps<T> splitTreaps = split(implicitTreapNode); // (T0, T1), where implicitTreapNode is in T0;


        if (isLessThanOrEqualTo)
            return splitTreaps;

        ImplicitTreapNode<T> treap0 = splitTreaps.getFirstSubtreeRoot(), treap1 = splitTreaps.getSecondSubtreeRoot();

        SplitTreaps<T> temp = split(treap0, sizeOfTreap(treap0) - 1); // T0 is split into (T0', implicitTreapNode)
        treap1 = join2(temp.getSecondSubtreeRoot(), treap1); // T1' is (implicitTreapNode, T1)

        return splitTreaps.setFirstSubtreeRoot(temp.getFirstSubtreeRoot()).setSecondSubtreeRoot(treap1); // return (T0', T1')
    }

    public static <T> ImplicitTreapNode<T> removeFirst(ImplicitTreapNode<T> implicitTreapNode) {
        return split(implicitTreapNode.getRoot(), 1).getSecondSubtreeRoot();
    }

    public static <T> ImplicitTreapNode<T> removeLast(ImplicitTreapNode<T> implicitTreapNode) {
        return split(implicitTreapNode.getRoot(), sizeOfTreap(implicitTreapNode) - 1).getFirstSubtreeRoot();
    }


    /**
     * Join two implicit treaps, where assuming every node in the left treap is smaller than every node in the right treap.
     * The implementation always assign the root with a lower priority to being a child of the root with a higher priority.
     *
     * @param leftTreap  the smaller treap
     * @param rightTreap the larger treap
     * @return the root of the joined treap
     */
    public static <T> ImplicitTreapNode<T> join(ImplicitTreapNode<T> leftTreap, ImplicitTreapNode<T> rightTreap) {
        if (leftTreap == null)
            if (rightTreap == null)
                return null;
            else
                return rightTreap.getRoot();
        else if (rightTreap == null)
            return leftTreap.getRoot();
        else
            return join2(leftTreap.getRoot(), rightTreap.getRoot());
    }

    /**
     * Join a sequence of treaps, where assuming that the treaps have already been sorted.
     *
     * @param treapRoots the sequence of treap nodes to be joined
     * @return the root of the joined treap
     */
    @SafeVarargs
    public static <T> ImplicitTreapNode<T> joinTreaps(ImplicitTreapNode<T>... treapRoots) {
        ImplicitTreapNode<T> ret = null;
        for (ImplicitTreapNode<T> treapNode : treapRoots)
            if (treapNode != null)
                ret = join2(ret, treapNode.getRoot());

        return ret;
    }

    public static <T> ImplicitTreapNode<T> joinTreaps(List<ImplicitTreapNode<T>> implicitTreapNodeList) {
        ImplicitTreapNode<T> ret = null;
        for (ImplicitTreapNode<T> treapNode : implicitTreapNodeList)
            ret = join2(ret, treapNode);
        return ret;
    }

    private static <T> ImplicitTreapNode<T> join2(ImplicitTreapNode<T> left, ImplicitTreapNode<T> right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        if (left.getPriority() < right.getPriority()) {
            right.setLeftChild(join2(left, right.getLeftChild()));
            return right;
        } else {
            left.setRightChild(join2(left.getRightChild(), right));
            return left;
        }
    }

    public static <T> void print(ImplicitTreapNode<T> implicitTreapNode) {
        if (implicitTreapNode != null)
            inorder(implicitTreapNode.getRoot());

    }

    private static <T> void inorder(ImplicitTreapNode<T> implicitTreapNode) {
        if (implicitTreapNode == null)
            return;
        inorder(implicitTreapNode.getLeftChild());
        System.out.println(implicitTreapNode);
        inorder(implicitTreapNode.getRightChild());
    }
}
