package zoo.common;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public final class ConnectedZooKeeper implements Watcher, AutoCloseable {
    private final CountDownLatch connected = new CountDownLatch(1);
    private final ZooKeeper zooKeeper;

    private ConnectedZooKeeper(String hostPort, Duration timeout) throws IOException {
        this.zooKeeper = new ZooKeeper(hostPort, Math.toIntExact(timeout.toMillis()), this);
    }

    public static ConnectedZooKeeper connect(String hostPort, Duration timeout) throws Exception {
        ConnectedZooKeeper client = new ConnectedZooKeeper(hostPort, timeout);
        if (!client.connected.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            client.close();
            throw new IllegalStateException("Could not connect to ZooKeeper at " + hostPort);
        }
        return client;
    }

    public ZooKeeper get() {
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connected.countDown();
        }
    }

    @Override
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
