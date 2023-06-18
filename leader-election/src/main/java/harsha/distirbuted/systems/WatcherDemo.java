package harsha.distirbuted.systems;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.List;



public class WatcherDemo implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String TARGET_ZNODE="/target_znode";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatcherDemo watcher = new WatcherDemo();
        watcher.connectToZookeeper();
        watcher.watchTargetZnode();
        watcher.run();
        watcher.close();
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }
    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat=zooKeeper.exists(TARGET_ZNODE,this);
        if (stat==null){
            return;
        }
        byte []data=zooKeeper.getData(TARGET_ZNODE,this,stat);
        List<String> children=zooKeeper.getChildren(TARGET_ZNODE,this);
        System.out.println("DATA: "+new String(data)+" children : "+children);
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
            case NodeDeleted:
                System.out.println(TARGET_ZNODE+" was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE+" Was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE+" Data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE+"Children changed");
                break;
        }
        try{
            watchTargetZnode();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
