package harsha.distirbuted.systems.serviceregistry;

import harsha.distirbuted.systems.serviceregistry.cluster.management.LeaderElection;
import harsha.distirbuted.systems.serviceregistry.cluster.management.OnElectionCallback;
import harsha.distirbuted.systems.serviceregistry.cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final int DEFAULT_PORT=8080;

    public static void main(String [] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort=args.length==1?Integer.parseInt(args[0]):DEFAULT_PORT;
        Application app =new Application();
        ZooKeeper zooKeeper=app.connectToZookeeper();
        ServiceRegistry serviceRegistry=new ServiceRegistry(zooKeeper);
        OnElectionCallback onElectionAction=new OnElectionAction(serviceRegistry,currentServerPort);
        LeaderElection election=new LeaderElection(zooKeeper,onElectionAction);
        election.volunteerForLeadership();
        election.reelectLeader();
        app.run();
        app.close();
        System.out.println("Disconnected from zookeeper , exiting application");
    }


    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnect Event from zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
                break;
        }
    }
}

