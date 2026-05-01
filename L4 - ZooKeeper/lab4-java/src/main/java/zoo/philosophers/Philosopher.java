package zoo.philosophers;

import java.time.Duration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import zoo.common.ConnectedZooKeeper;
import zoo.common.ZooKeeperPaths;

public final class Philosopher {
    private final String name;
    private final String hostPort;
    private final int philosopherId;
    private final int philosopherCount;
    private final String root;

    public Philosopher(String name, String hostPort, int philosopherId, int philosopherCount, String root) {
        if (philosopherCount < 2) {
            throw new IllegalArgumentException("At least two philosophers are required");
        }
        if (philosopherId < 0 || philosopherId >= philosopherCount) {
            throw new IllegalArgumentException("philosopherId must be in [0, philosopherCount)");
        }
        this.name = name;
        this.hostPort = hostPort;
        this.philosopherId = philosopherId;
        this.philosopherCount = philosopherCount;
        this.root = root;
    }

    public boolean dine(Duration waitTimeout, Duration eatTime) throws Exception {
        try (ConnectedZooKeeper connection = ConnectedZooKeeper.connect(hostPort, Duration.ofSeconds(5))) {
            ZooKeeper zooKeeper = connection.get();
            ZooKeeperPaths.ensurePath(zooKeeper, root + "/forks");
            ZooKeeperPaths.ensurePath(zooKeeper, root + "/eating");

            int leftFork = philosopherId;
            int rightFork = (philosopherId + 1) % philosopherCount;
            int firstFork = Math.min(leftFork, rightFork);
            int secondFork = Math.max(leftFork, rightFork);

            try (DistributedLock first = new DistributedLock(zooKeeper, forkPath(firstFork));
                 DistributedLock second = new DistributedLock(zooKeeper, forkPath(secondFork))) {
                if (!first.acquire(waitTimeout)) {
                    return false;
                }
                if (!second.acquire(waitTimeout)) {
                    return false;
                }
                eat(zooKeeper, eatTime);
                return true;
            }
        }
    }

    private void eat(ZooKeeper zooKeeper, Duration eatTime) throws Exception {
        String eatingPath = root + "/eating/" + name;
        zooKeeper.create(eatingPath, ZooKeeperPaths.bytes(name), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        try {
            Thread.sleep(eatTime.toMillis());
        } finally {
            ZooKeeperPaths.deleteIfExists(zooKeeper, eatingPath);
        }
    }

    private String forkPath(int forkId) {
        return root + "/forks/fork-" + forkId;
    }
}
