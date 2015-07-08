package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation for LoadProgressManager.
 * Thresholds and maximum number of tries should be set as Hadoop configurations.
 */
public final class LoadProgressManagerImpl implements LoadProgressManager {

  private static final Logger LOG = Logger.getLogger(LoadProgressManagerImpl.class.getName());

  /**
   * The epsilon below which a cache is considered to not make progress (in bytes per second)
   */
  public static final String LOAD_NO_PROGRESS_KEY = "surf.load.progress.no.bps";
  public static final long LOAD_NO_PROGRESS_DEFAULT = 1024;

  /**
   * The threshold above which a cache is considered to make OK progress (in bytes per second).
   * When below this threshold and above no progress, a new cache will be tried.
   */
  public static final String LOAD_OK_PROGRESS_KEY = "surf.load.progress.ok.bps";
  public static final long LOAD_OK_PROGRESS_DEFAULT = 10 * 1024 * 1024;

  /**
   * When no progress is made this number of times, the cache is no longer considered
   */
  public static final String LOAD_MAX_NO_PROGRESS_KEY = "surf.load.progress.no.max";
  public static final int LOAD_MAX_NO_PROGRESS_DEFAULT = 2;

  /**
   * When file is not found this number of times, the cache is no longer considered
   */
  public static final String LOAD_MAX_NOT_FOUND_KEY = "surf.load.notfound.max";
  public static final int LOAD_MAX_NOT_FOUND_DEFAULT = 2;

  /**
   * When connection fails this number of times, the cache is no longer considered
   */
  public static final String LOAD_MAX_NOT_CONNECTED_KEY = "surf.load.notconnected.max";
  public static final int LOAD_MAX_NOT_CONNECTED_DEFAULT = 1;

  private long noProgress;
  private long okProgress;
  private int maxNoProgress;
  private int maxNotFound;
  private int maxNotConnected;

  private List<String> activeCaches;

  private long startTime;
  private Map<String, Progress> cacheProgress;

  @Override
  public void initialize(final List<NodeInfo> addresses, Configuration conf) {
    activeCaches = new ArrayList<>(addresses.size());
    for (NodeInfo node : addresses) {
      activeCaches.add(node.getAddress());
    }

    startTime = System.currentTimeMillis();
    cacheProgress = new HashMap<>();

    noProgress = conf.getLong(LOAD_NO_PROGRESS_KEY, LOAD_NO_PROGRESS_DEFAULT);
    okProgress = conf.getLong(LOAD_OK_PROGRESS_KEY, LOAD_OK_PROGRESS_DEFAULT);
    maxNoProgress = conf.getInt(LOAD_MAX_NO_PROGRESS_KEY, LOAD_MAX_NO_PROGRESS_DEFAULT);
    maxNotFound = conf.getInt(LOAD_MAX_NOT_FOUND_KEY, LOAD_MAX_NOT_FOUND_DEFAULT);
    maxNotConnected = conf.getInt(LOAD_MAX_NOT_CONNECTED_KEY, LOAD_MAX_NOT_CONNECTED_DEFAULT);
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
    LOG.log(Level.FINE, "From address {0}, loading bytes per second {1}",
            new String[]{address, Long.toString(bytesPerSecond)});

    if (bytesPerSecond < noProgress) {
      final int noProgressCount = progress.getNoProgressCount() + 1;

      if (noProgressCount == maxNoProgress) {
        activeCaches.remove(address);
        cacheProgress.remove(address);
      } else {
        progress.setNoProgressCount(noProgressCount);
      }
    } else if (bytesPerSecond < okProgress) {
      // Move to back, so another cache can be chosen
      activeCaches.remove(address);
      activeCaches.add(address);
    }
  }

  @Override
  public void notFound(String address) {
    final Progress currProgress = cacheProgress.get(address);
    final int notFoundCount = currProgress.getNotFoundCount() + 1;

    if (notFoundCount == maxNotFound) {
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

    if (notConnectedCount == maxNotConnected) {
      activeCaches.remove(address);
      cacheProgress.remove(address);
    } else {
      currProgress.setNotConnectedCount(notConnectedCount);
    }
  }

  @Override
  public String getNextCache() throws IOException {
    if (activeCaches.size() == 0) {
      throw new IOException("No Cache available");
    } else {
      final String nextCache = activeCaches.get(0);
      if (!cacheProgress.containsKey(nextCache)) {
        final Progress currProgress = new Progress(startTime, 0);
        cacheProgress.put(nextCache, currProgress);
      }
      return nextCache;
    }
  }

  /**
   * Keep track of number of tries and bytes loaded at the previous measured time
   */
  private static final class Progress {
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
