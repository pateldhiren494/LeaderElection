import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class LeaderElection implements Watcher {
    private static final String SERVER_ENDPOINT = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    public static void main(String[] args) {
        try {
            LeaderElection leaderElection = new LeaderElection();
            leaderElection.connectToZookeeper();
            leaderElection.run();
            leaderElection.close();
            System.out.println("Disconnected from zookeeper");
        } catch(Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(SERVER_ENDPOINT, SESSION_TIMEOUT, this);
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None: {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println(("Successfully connected to zookeepr"));
                } else {
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                }
            }
        }
    }
}
