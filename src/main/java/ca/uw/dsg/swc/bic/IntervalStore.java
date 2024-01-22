package ca.uw.dsg.swc.bic;

import static ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity.base;

public class IntervalStore {
    private final boolean[] data;

    public IntervalStore(int size) {
        this.data = new boolean[size];
    }

    public void insert(int l, int h) {
//        for (int i = l; i <= h; i++)
//            data[i] = true;
        System.arraycopy(base, l, data, l, h - l + 1);
    }

    public boolean containsKey(int k) {
        return data[k];
    }

    public void insertAll(IntervalStore intervalStore) {
        boolean[] other = intervalStore.data;
        for (int i = 0, len = other.length; i < len; i++) {
            if (other[i])
                data[i] = true;
        }
    }

}
