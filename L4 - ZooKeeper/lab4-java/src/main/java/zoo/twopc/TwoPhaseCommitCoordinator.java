package zoo.twopc;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import zoo.common.ConnectedZooKeeper;
import zoo.common.ZooKeeperPaths;

public final class TwoPhaseCommitCoordinator {
    private final String hostPort;
    private final String root;

    public TwoPhaseCommitCoordinator(String hostPort, String root) {
        this.hostPort = hostPort;
        this.root = root;
    }

    public Decision coordinate(String transactionId, String proposedValue, int participantCount, Duration timeout)
            throws Exception {
        try (ConnectedZooKeeper connection = ConnectedZooKeeper.connect(hostPort, Duration.ofSeconds(5))) {
            ZooKeeper zooKeeper = connection.get();
            String txPath = root + "/tx/" + transactionId;
            prepareTransaction(zooKeeper, txPath, proposedValue);

            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                List<String> votes = zooKeeper.getChildren(txPath + "/votes", false);
                if (containsAbort(zooKeeper, txPath, votes)) {
                    return publishDecision(zooKeeper, txPath, Decision.ABORT);
                }
                if (votes.size() >= participantCount && allCommit(zooKeeper, txPath, votes)) {
                    return publishDecision(zooKeeper, txPath, Decision.COMMIT);
                }

                long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    return publishDecision(zooKeeper, txPath, Decision.ABORT);
                }

                CountDownLatch changed = new CountDownLatch(1);
                zooKeeper.getChildren(txPath + "/votes", (WatchedEvent event) -> changed.countDown());
                changed.await(Math.min(remainingMillis, 250L), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void prepareTransaction(ZooKeeper zooKeeper, String txPath, String proposedValue) throws Exception {
        ZooKeeperPaths.ensurePath(zooKeeper, root + "/tx");
        if (zooKeeper.exists(txPath, false) == null) {
            try {
                zooKeeper.create(txPath, ZooKeeperPaths.bytes(proposedValue), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
                zooKeeper.setData(txPath, ZooKeeperPaths.bytes(proposedValue), -1);
            }
        } else {
            zooKeeper.setData(txPath, ZooKeeperPaths.bytes(proposedValue), -1);
        }
        ZooKeeperPaths.ensurePath(zooKeeper, txPath + "/votes");
        ZooKeeperPaths.ensurePath(zooKeeper, txPath + "/acks");
    }

    private boolean containsAbort(ZooKeeper zooKeeper, String txPath, List<String> votes) throws Exception {
        for (String vote : votes) {
            if (Decision.ABORT.name().equals(readVote(zooKeeper, txPath, vote))) {
                return true;
            }
        }
        return false;
    }

    private boolean allCommit(ZooKeeper zooKeeper, String txPath, List<String> votes) throws Exception {
        for (String vote : votes) {
            if (!Decision.COMMIT.name().equals(readVote(zooKeeper, txPath, vote))) {
                return false;
            }
        }
        return true;
    }

    private String readVote(ZooKeeper zooKeeper, String txPath, String participant) throws Exception {
        return ZooKeeperPaths.string(zooKeeper.getData(txPath + "/votes/" + participant, false, null));
    }

    private Decision publishDecision(ZooKeeper zooKeeper, String txPath, Decision decision) throws Exception {
        String decisionPath = txPath + "/decision";
        if (zooKeeper.exists(decisionPath, false) == null) {
            try {
                zooKeeper.create(decisionPath, ZooKeeperPaths.bytes(decision.name()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
                zooKeeper.setData(decisionPath, ZooKeeperPaths.bytes(decision.name()), -1);
            }
        } else {
            zooKeeper.setData(decisionPath, ZooKeeperPaths.bytes(decision.name()), -1);
        }
        return decision;
    }
}
