package harsha.distirbuted.systems.cluster.management;

public interface OnElectionCallback {
    void onElectToBeLeader();
    void onWorker();
}
