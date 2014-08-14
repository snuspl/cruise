package org.apache.reef.inmemory.client.cli;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.client.SurfFS;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class CLIUtils {

  /**
   * Return the recursive list of files under a directory.
   * If the pathString provided is not a directory, an empty list is provided with that status.
   * @param surfFs
   * @param pathString Path the recursively list
   * @return
   * @throws IOException
   * @throws TException
   */
  public static List<FileStatus> getRecursiveList(SurfFS surfFs, String pathString) throws IOException, TException {
    final Path rootPath = new Path(pathString);
    final FileStatus rootStatus = surfFs.getFileStatus(rootPath);
    final List<FileStatus> files = new LinkedList<>();
    if (!rootStatus.isDirectory()) {
      files.add(rootStatus);
    } else {
      final List<FileStatus> dirsToRecurse = new LinkedList<>();
      dirsToRecurse.add(rootStatus);

      while (dirsToRecurse.size() > 0) {
        final FileStatus dir = dirsToRecurse.remove(0);
        final FileStatus[] statuses = surfFs.listStatus(dir.getPath());
        for (final FileStatus status : statuses) {
          if (status.isFile()) {
            files.add(status);
          } else if (status.isDirectory()) {
            dirsToRecurse.add(status);
          }
        }
      }
    }
    return files;
  }
}
