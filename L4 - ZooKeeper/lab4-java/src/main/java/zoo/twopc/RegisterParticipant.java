package zoo.twopc;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import zoo.common.ConnectedZooKeeper;
import zoo.common.ZooKeeperPaths;

public final class RegisterParticipant {
    private final String name;
    private final String hostPort;
    private final String root;
    private String committedValue;

    public RegisterParticipant(String name, String hostPort, String root, String initialValue) {
        this.name = name;
        this.hostPort = hostPort;
        this.root = root;
        this.committedValue = initialValue;
    }

    public String committedValue() {
        return committedValue;
    }

    public Decision voteAndApply(String transactionId, Decision vote, Duration timeout) throws Exception {
        try (ConnectedZooKeeper connection = ConnectedZooKeeper.connect(hostPort, Duration.ofSeconds(5))) {
            ZooKeeper zooKeeper = connection.get();
            String txPath = root + "/tx/" + transactionId;
            waitForPath(zooKeeper, txPath, timeout);
            String proposedValue = ZooKeeperPaths.string(zooKeeper.getData(txPath, false, null));
            publishVote(zooKeeper, txPath, vote);
            Decision decision = awaitDecision(zooKeeper, txPath, timeout);
            if (decision == Decision.COMMIT) {
                committedValue = proposedValue;
            }
            publishAck(zooKeeper, txPath, decision);
            return decision;
        }
    }

    private void publishVote(ZooKeeper zooKeeper, String txPath, Decision vote) throws Exception {
        ZooKeeperPaths.ensurePath(zooKeeper, txPath + "/votes");
        String votePath = txPath + "/votes/" + name;
        if (zooKeeper.exists(votePath, false) == null) {
            try {
                zooKeeper.create(votePath, ZooKeeperPaths.bytes(vote.name()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException ignored) {
                zooKeeper.setData(votePath, ZooKeeperPaths.bytes(vote.name()), -1);
            }
        } else {
            zooKeeper.setData(votePath, ZooKeeperPaths.bytes(vote.name()), -1);
        }
    }

    private Decision awaitDecision(ZooKeeper zooKeeper, String txPath, Duration timeout) throws Exception {
        String decisionPath = txPath + "/decision";
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (zooKeeper.exists(decisionPath, false) != null) {
                return Decision.valueOf(ZooKeeperPaths.string(zooKeeper.getData(decisionPath, false, null)));
            }

            long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0) {
                return Decision.ABORT;
            }

            CountDownLatch changed = new CountDownLatch(1);
            if (zooKeeper.exists(decisionPath, (WatchedEvent event) -> changed.countDown()) != null) {
                continue;
            }
            changed.await(Math.min(remainingMillis, 250L), TimeUnit.MILLISECONDS);
        }
    }

    private void publishAck(ZooKeeper zooKeeper, String txPath, Decision decision) throws Exception {
        ZooKeeperPaths.ensurePath(zooKeeper, txPath + "/acks");
        String ackPath = txPath + "/acks/" + name;
        String ack = decision == Decision.COMMIT ? "COMMITTED" : "ABORTED";
        if (zooKeeper.exists(ackPath, false) == null) {
            zooKeeper.create(ackPath, ZooKeeperPaths.bytes(ack), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.setData(ackPath, ZooKeeperPaths.bytes(ack), -1);
        }
    }

    private void waitForPath(ZooKeeper zooKeeper, String path, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (zooKeeper.exists(path, false) == null) {
            long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0) {
                throw new IllegalStateException("Transaction path was not created: " + path);
            }
            CountDownLatch created = new CountDownLatch(1);
            String parent = path.substring(0, path.lastIndexOf('/'));
            if (zooKeeper.exists(parent, false) == null) {
                Thread.sleep(Math.min(remainingMillis, 100L));
                continue;
            }
            zooKeeper.getChildren(parent, (WatchedEvent event) -> created.countDown());
            created.await(Math.min(remainingMillis, 250L), TimeUnit.MILLISECONDS);
        }
    }
}
