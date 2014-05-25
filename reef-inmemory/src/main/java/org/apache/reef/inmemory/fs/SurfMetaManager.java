package org.apache.reef.inmemory.fs;

import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Surf FileSystem Meta Information management
 */
public class SurfMetaManager {
  private static SortedMap<Path, org.apache.reef.inmemory.fs.entity.FileMeta> fsMeta;
  public static String USERS_HOME = "/user";

  static {
    fsMeta = new TreeMap<Path, org.apache.reef.inmemory.fs.entity.FileMeta>();
  }

  public List<org.apache.reef.inmemory.fs.entity.FileMeta> listStatus(Path path, boolean recursive, org.apache.reef.inmemory.fs.entity.User creator) throws FileNotFoundException {
    //TODO: need to support glob pattern
    List<org.apache.reef.inmemory.fs.entity.FileMeta> fm = new ArrayList<org.apache.reef.inmemory.fs.entity.FileMeta>();
    Path newPath = null;

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

    if (!fsMeta.containsKey(newPath))
      throw new FileNotFoundException(path + " does not exists");

    //TODO: Authority check will be needed

    SortedMap<Path, org.apache.reef.inmemory.fs.entity.FileMeta> subMap = fsMeta.tailMap(newPath);

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

  public org.apache.reef.inmemory.fs.entity.FileMeta makeDirectory(Path path, org.apache.reef.inmemory.fs.entity.User creator) throws FileAlreadyExistsException {
    Path newPath = null;
    org.apache.reef.inmemory.fs.entity.FileMeta fm = new org.apache.reef.inmemory.fs.entity.FileMeta();
    fm.setOwner(creator);
    fm.setDirectory(true);

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

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

  public boolean delete(Path path, boolean recursive, org.apache.reef.inmemory.fs.entity.User creator) throws IOException {
    Path newPath = null;
    org.apache.reef.inmemory.fs.entity.FileMeta fm = new org.apache.reef.inmemory.fs.entity.FileMeta();
    fm.setOwner(creator);
    fm.setDirectory(true);

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

    fm.setFullPath(newPath.toString());

    //if fsMeta has path, throw FileAlreadyExistsException
    if (!fsMeta.containsKey(newPath))
      throw new FileNotFoundException(path + " dose not exist");

    if (newPath.isRoot()) {
      throw new IOException(path + " can't be deleted.");
    }

    //TODO: Authority check will be needed

    SortedMap<Path, org.apache.reef.inmemory.fs.entity.FileMeta> subMap = fsMeta.tailMap(newPath);
    Path[] keys = subMap.keySet().toArray(new Path[subMap.size()]);
    String prefix = newPath.toString();

    for (Path subPath : keys) {
      if (subPath.toString().equals(prefix) || subPath.toString().startsWith(prefix + Path.SEPARATOR)) {
        fsMeta.remove(subPath);
      }
    }

    return true;
  }
}
