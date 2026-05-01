package zoo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

final class EmbeddedZooKeeper implements AutoCloseable {
    private final Path dataDir;
    private final ZooKeeperServer server;
    private final NIOServerCnxnFactory factory;
    private final int port;

    EmbeddedZooKeeper() throws Exception {
        this.dataDir = Files.createTempDirectory("lab4-zookeeper-test");
        this.port = findFreePort();
        this.server = new ZooKeeperServer(
                dataDir.resolve("snapshot").toFile(),
                dataDir.resolve("log").toFile(),
                2_000);
        this.factory = new NIOServerCnxnFactory();
        this.factory.configure(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 64);
        this.factory.startup(server);
    }

    String connectString() {
        return "127.0.0.1:" + port;
    }

    @Override
    public void close() throws Exception {
        factory.shutdown();
        server.shutdown();
        Thread.sleep(100L);
        deleteDirectory(dataDir);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void deleteDirectory(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        for (int attempt = 0; attempt < 5; attempt++) {
            try (var paths = Files.walk(path)) {
                paths.sorted(Comparator.reverseOrder()).forEach(current -> {
                    try {
                        Files.deleteIfExists(current);
                    } catch (IOException e) {
                        current.toFile().deleteOnExit();
                    }
                });
            }
            if (!Files.exists(path)) {
                return;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        path.toFile().deleteOnExit();
    }
}
