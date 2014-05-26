package org.apache.reef.inmemory.fs;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Surf FileSystem Meta Information management
 */
public class SurfMetaManager {
  private static SortedMap<Path, FileMeta> fsMeta;
  public static String USERS_HOME = "/user";

  static {
    fsMeta = new TreeMap<Path, FileMeta>();
  }

  public List<FileMeta> listStatus(Path path, boolean recursive, User creator) throws FileNotFoundException {
    //TODO: need to support glob pattern
    List<FileMeta> fm = new ArrayList<FileMeta>();
    Path newPath = getAbsolutePath(path, creator);

    if (!fsMeta.containsKey(newPath))
      throw new FileNotFoundException(path + " does not exists");

    //TODO: Authority check will be needed

    SortedMap<Path, FileMeta> subMap = fsMeta.tailMap(newPath);

    for (Path subPath : subMap.keySet()) {
      if (subPath.toString().contains(newPath.toString())) {
        if (subPath.isRoot() || subPath.getParent().depth() == newPath.depth())
          fm.add(subMap.get(subPath));
        else if (recursive && subPath.depth() >= newPath.depth())
          fm.add(subMap.get(subPath));
      }
    }

    return fm;
  }

  public FileMeta makeDirectory(Path path, User creator) throws FileAlreadyExistsException {
    FileMeta fm = new FileMeta();
    fm.setOwner(creator);
    fm.setDirectory(true);
    Path newPath = getAbsolutePath(path, creator);
    fm.setFullPath(newPath.toString());

    //if fsMeta has path, throw FileAlreadyExistsException
    if (fsMeta.containsKey(newPath))
      throw new FileAlreadyExistsException(path + " already exists");

    //TODO: Authority check will be needed

    //With recursive option, create parent directory recursively.
    if (!newPath.isRoot()) {
      try {
        makeDirectory(newPath.getParent(), creator);
      } catch (FileAlreadyExistsException e) {
      }
    }

    fsMeta.put(newPath, fm);

    return fm;
  }

  public boolean delete(Path path, boolean recursive, User creator) throws IOException {
    Path newPath = getAbsolutePath(path, creator);

    if (!fsMeta.containsKey(newPath))
      throw new FileNotFoundException(path + " dose not exist");

    if (newPath.isRoot()) {
      throw new IOException(path + " can't be deleted.");
    }

    //TODO: Authority check will be needed

    SortedMap<Path, FileMeta> subMap = fsMeta.tailMap(newPath);
    Path[] keys = subMap.keySet().toArray(new Path[subMap.size()]);
    String prefix = newPath.toString();

    for (Path subPath : keys) {
      if (subPath.toString().equals(prefix) || subPath.toString().startsWith(prefix + Path.SEPARATOR)) {
        fsMeta.remove(subPath);
      }
    }

    return true;
  }

  public boolean rename(Path src, Path dst, User creator) throws IOException {
    Path newSrc = getAbsolutePath(src, creator);
    Path newDst = getAbsolutePath(dst, creator);

    if (!fsMeta.containsKey(newSrc))
      throw new FileNotFoundException(newSrc + " dose not exist");
    if (fsMeta.containsKey(newDst))
      throw new FileAlreadyExistsException(newDst + " already exist");

    if (newSrc.isRoot())
      throw new IOException(src + " can't be moved.");

    //TODO: Authority check will be needed

    SortedMap<Path, FileMeta> subMap = fsMeta.tailMap(newSrc);
    Path[] keys = subMap.keySet().toArray(new Path[subMap.size()]);
    String prefix = newSrc.toString();
    String newPrefix = newDst.toString();

    for (Path subPath : keys) {
      if (subPath.toString().equals(prefix) || subPath.toString().startsWith(prefix + Path.SEPARATOR)) {
        FileMeta fm = fsMeta.remove(subPath);
        fm.setFullPath(fm.getFullPath().replaceFirst(prefix, newPrefix));
        fsMeta.put(new Path(fm.getFullPath()), fm);
      }
    }

    return true;
  }

  private Path getAbsolutePath(Path path, User creator) {
    Path newPath = null;

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

    return newPath;
  }
}
