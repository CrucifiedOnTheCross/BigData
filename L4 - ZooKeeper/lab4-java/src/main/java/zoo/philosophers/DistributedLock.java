package zoo.philosophers;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import zoo.common.ZooKeeperPaths;

public final class DistributedLock implements AutoCloseable {
    private final ZooKeeper zooKeeper;
    private final String lockRoot;
    private String lockPath;

    public DistributedLock(ZooKeeper zooKeeper, String lockRoot) {
        this.zooKeeper = zooKeeper;
        this.lockRoot = lockRoot;
    }

    public boolean acquire(Duration timeout) throws Exception {
        ZooKeeperPaths.ensurePath(zooKeeper, lockRoot);
        lockPath = zooKeeper.create(
                lockRoot + "/lock-",
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        long deadline = System.nanoTime() + timeout.toNanos();
        String ownNode = lockPath.substring(lockRoot.length() + 1);
        while (true) {
            List<String> children = zooKeeper.getChildren(lockRoot, false)
                    .stream()
                    .sorted(Comparator.naturalOrder())
                    .toList();
            int ownIndex = children.indexOf(ownNode);
            if (ownIndex == 0) {
                return true;
            }
            if (ownIndex < 0) {
                throw new IllegalStateException("Lock node disappeared: " + lockPath);
            }

            CountDownLatch predecessorDeleted = new CountDownLatch(1);
            String predecessor = lockRoot + "/" + children.get(ownIndex - 1);
            if (zooKeeper.exists(predecessor, (WatchedEvent event) -> predecessorDeleted.countDown()) == null) {
                continue;
            }

            long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0) {
                release();
                return false;
            }
            predecessorDeleted.await(remainingMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void release() throws Exception {
        if (lockPath != null && zooKeeper.exists(lockPath, false) != null) {
            zooKeeper.delete(lockPath, -1);
            lockPath = null;
        }
    }

    @Override
    public void close() throws Exception {
        release();
    }
}
