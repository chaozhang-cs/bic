package ca.uw.dsg.swc.baselines;


public interface FullyDynamicConnectivity {

    boolean connected(int source, int target);

    void insertEdge(int source, int target);

    void deleteEdge(int source, int target);

    String getName();
}
