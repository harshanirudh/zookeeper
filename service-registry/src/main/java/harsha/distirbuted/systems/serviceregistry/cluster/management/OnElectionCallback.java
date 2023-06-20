package harsha.distirbuted.systems.serviceregistry.cluster.management;

public interface OnElectionCallback {
    void onElectToBeLeader();
    void onWorker();
}
