package zoo.common;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public final class ZooKeeperPaths {
    private ZooKeeperPaths() {
    }

    public static void ensurePath(ZooKeeper zooKeeper, String path) throws Exception {
        if ("/".equals(path)) {
            return;
        }

        StringBuilder current = new StringBuilder();
        for (String part : path.split("/")) {
            if (part.isBlank()) {
                continue;
            }
            current.append('/').append(part);
            String currentPath = current.toString();
            if (zooKeeper.exists(currentPath, false) == null) {
                try {
                    zooKeeper.create(
                            currentPath,
                            new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException ignored) {
                    // Another process created the path between exists() and create().
                }
            }
        }
    }

    public static void deleteIfExists(ZooKeeper zooKeeper, String path) throws Exception {
        if (zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, -1);
        }
    }

    public static void deleteRecursiveIfExists(ZooKeeper zooKeeper, String path) throws Exception {
        if (zooKeeper.exists(path, false) == null) {
            return;
        }
        List<String> children = zooKeeper.getChildren(path, false);
        for (String child : children) {
            deleteRecursiveIfExists(zooKeeper, path + "/" + child);
        }
        zooKeeper.delete(path, -1);
    }

    public static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
