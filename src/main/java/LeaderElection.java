import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String SERVER_ENDPOINT = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws Exception {
        //try {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from zookeeper");
        /*} catch(Exception ex) {
            System.out.println(ex.getMessage());
        }*/
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

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Znode name "  + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;

        while(predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            if (children.get(0).equals(currentZnodeName)) {
                System.out.println("I am the Leader" + ELECTION_NAMESPACE + "/" + currentZnodeName);
                return;
            } else {
                System.out.println("I am not the Leader" + ELECTION_NAMESPACE + "/" + currentZnodeName);
                if (currentZnodeName != null) {
                    int index = children.indexOf(currentZnodeName);
                    predecessorStat = watchZnode(children.get(index - 1));
                }
            }
        }
    }

    private Stat watchZnode(String znodeToWatch) throws InterruptedException, KeeperException {
        if (znodeToWatch == null) return null;

        Stat stat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + znodeToWatch, this);
        //if (stat == null) return;
        //byte[] data = zooKeeper.getData(ELECTION_NAMESPACE + "/" + znodeToWatch, this, stat);
        //List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE + "/" + znodeToWatch, this);

        System.out.println("Watching Znode " + ELECTION_NAMESPACE + "/" + znodeToWatch);
        return stat;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None: {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println(("Successfully connected to zookeeper"));
                } else {
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                }
                break;
            }
            case NodeCreated:
                System.out.println("NodeCreated");
                break;
            case NodeDeleted:
                try {
                    electLeader();
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
                System.out.println("Predecessor node deleted");
                break;
            case NodeDataChanged:
                System.out.println("NodeDataChanged");
                break;
            case NodeChildrenChanged:
                System.out.println("NodeChildrenChanged");
                break;
        }
    }
}
