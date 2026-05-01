package ru.bigdata.lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Spark on Windows tries to call Hadoop permission helpers that depend on winutils.exe.
 * For this laboratory we write to the local filesystem only, so skipping chmod calls is enough.
 */
public class NoOpPermissionLocalFileSystem extends RawLocalFileSystem {

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        // Intentionally no-op to avoid the winutils.exe dependency on Windows.
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File directory = pathToFile(path);
        if (!directory.exists()) {
            throw new FileNotFoundException("Path does not exist: " + path);
        }

        if (directory.isFile()) {
            return new FileStatus[]{getFileStatus(path)};
        }

        File[] children = directory.listFiles();
        if (children == null) {
            throw new IOException("Cannot list path: " + path);
        }

        FileStatus[] statuses = new FileStatus[children.length];
        for (int index = 0; index < children.length; index++) {
            statuses[index] = toFileStatus(children[index], new Path(path, children[index].getName()));
        }
        return statuses;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException("Path does not exist: " + path);
        }
        return toFileStatus(file, path);
    }

    private FileStatus toFileStatus(File file, Path path) {
        boolean directory = file.isDirectory();
        long length = directory ? 0L : file.length();
        Path qualifiedPath = path.makeQualified(getUri(), getWorkingDirectory());
        return new FileStatus(
                length,
                directory,
                1,
                getDefaultBlockSize(path),
                file.lastModified(),
                qualifiedPath
        );
    }
}
