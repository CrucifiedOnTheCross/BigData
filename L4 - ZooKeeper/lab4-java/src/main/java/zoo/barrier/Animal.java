package zoo.barrier;

import java.time.Duration;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import zoo.common.ConnectedZooKeeper;
import zoo.common.ZooKeeperPaths;

public final class Animal implements Watcher, AutoCloseable {
    private final String name;
    private final String root;
    private final int partySize;
    private final ConnectedZooKeeper connection;
    private final ZooKeeper zooKeeper;
    private final Object mutex = new Object();
    private final String animalPath;

    public Animal(String name, String hostPort, String root, int partySize) throws Exception {
        if (partySize < 1) {
            throw new IllegalArgumentException("partySize must be positive");
        }
        this.name = name;
        this.root = root;
        this.partySize = partySize;
        this.connection = ConnectedZooKeeper.connect(hostPort, Duration.ofSeconds(5));
        this.zooKeeper = connection.get();
        this.animalPath = root + "/" + name;
    }

    public String name() {
        return name;
    }

    public boolean enter(Duration timeout) throws Exception {
        ZooKeeperPaths.ensurePath(zooKeeper, root);
        ZooKeeperPaths.deleteIfExists(zooKeeper, animalPath);
        zooKeeper.create(
                animalPath,
                ZooKeeperPaths.bytes(name),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (mutex) {
            while (true) {
                List<String> party = zooKeeper.getChildren(root, this);
                if (party.size() >= partySize) {
                    return true;
                }

                long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    return false;
                }
                mutex.wait(remainingMillis);
            }
        }
    }

    public void leave() throws Exception {
        try {
            ZooKeeperPaths.deleteIfExists(zooKeeper, animalPath);
        } catch (KeeperException.NoNodeException ignored) {
            // The ephemeral node may already be gone if the session closed.
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            System.out.println(name + " received ZooKeeper event: " + event.getType());
            mutex.notifyAll();
        }
    }

    @Override
    public void close() throws Exception {
        leave();
        connection.close();
    }
}
