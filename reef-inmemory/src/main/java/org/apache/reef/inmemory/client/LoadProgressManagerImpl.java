package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.NodeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LoadProgressManagerImpl implements LoadProgressManager {

  private static final Logger LOG = Logger.getLogger(LoadProgressManagerImpl.class.getName());

  // TODO: make these configurable?
  // bytes per second
  public static final long NO_PROGRESS = 1024;
  public static final long OK_PROGRESS = 10 * 1024 * 1024;

  public static final int MAX_NO_PROGRESS = 2;
  public static final int MAX_NOT_FOUND = 2;
  public static final int MAX_NOT_CONNECTED = 1;

  private List<String> activeCaches;

  private long startTime;
  private Map<String, Progress> cacheProgress;

  @Override
  public void initialize(final List<NodeInfo> addresses, long length) {
    activeCaches = new ArrayList<>(addresses.size());
    for (NodeInfo node : addresses) {
      activeCaches.add(node.getAddress());
    }

    startTime = System.currentTimeMillis();
    cacheProgress = new HashMap<>();
  }

  @Override
  public void loadingProgress(String address, long bytesLoaded) {
    final Progress progress = cacheProgress.get(address);
    final long prevTime = progress.getTime();
    final long prevBytes = progress.getBytesLoaded();

    final long time = System.currentTimeMillis();
    progress.setTime(time);
    progress.setBytesLoaded(bytesLoaded);

    final long bytesPerSecond = (long) (1000.0 * (bytesLoaded - prevBytes) / (time - prevTime));
    LOG.log(Level.INFO, "Bytes per second "+bytesPerSecond);
    if (bytesPerSecond < NO_PROGRESS) {
      final int noProgressCount = progress.getNoProgressCount() + 1;

      if (noProgressCount == MAX_NO_PROGRESS) {
        activeCaches.remove(address);
        cacheProgress.remove(address);
      } else {
        progress.setNoProgressCount(noProgressCount);
      }
    } else if (bytesPerSecond < OK_PROGRESS) {
      // Move to back, so another cache can be chosen
      activeCaches.remove(address);
      activeCaches.add(address);
    }
  }

  @Override
  public void notFound(String address) {
    final Progress currProgress = cacheProgress.get(address);
    final int notFoundCount = currProgress.getNotFoundCount() + 1;

    if (notFoundCount == MAX_NOT_FOUND) {
      activeCaches.remove(address);
      cacheProgress.remove(address);
    } else {
      currProgress.setNotFoundCount(notFoundCount);
    }
  }

  @Override
  public void notConnected(String address) {
    final Progress currProgress = cacheProgress.get(address);
    final int notConnectedCount = currProgress.getNotConnectedCount() + 1;
    currProgress.setNotConnectedCount(notConnectedCount);

    if (notConnectedCount == MAX_NOT_CONNECTED) {
      activeCaches.remove(address);
      cacheProgress.remove(address);
    } else {
      currProgress.setNotConnectedCount(notConnectedCount);
    }
  }

  @Override
  public String getNextCache() {
    if (activeCaches.size() == 0) {
      return null;
    } else {
      final String nextCache = activeCaches.get(0);
      if (!cacheProgress.containsKey(nextCache)) {
        final Progress currProgress = new Progress(startTime, 0);
        cacheProgress.put(nextCache, currProgress);
      }
      return nextCache;
    }
  }

  private static class Progress {
    private long time;
    private long bytesLoaded;

    private int noProgressCount = 0;
    private int notFoundCount = 0;
    private int notConnectedCount = 0;

    private Progress() {
    }

    private Progress(long time, long bytesLoaded) {
      this.time = time;
      this.bytesLoaded = bytesLoaded;
    }

    public long getTime() {
      return time;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public long getBytesLoaded() {
      return bytesLoaded;
    }

    public void setBytesLoaded(long bytesLoaded) {
      this.bytesLoaded = bytesLoaded;
    }

    public int getNoProgressCount() {
      return noProgressCount;
    }

    public void setNoProgressCount(int noProgressCount) {
      this.noProgressCount = noProgressCount;
    }

    public int getNotFoundCount() {
      return notFoundCount;
    }

    public void setNotFoundCount(int notFoundCount) {
      this.notFoundCount = notFoundCount;
    }

    public int getNotConnectedCount() {
      return notConnectedCount;
    }

    public void setNotConnectedCount(int notConnectedCount) {
      this.notConnectedCount = notConnectedCount;
    }
  }
}
