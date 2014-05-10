package org.apache.reef.inmemory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.reef.task.Task;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
public class InMemoryTask implements Task {
    private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());

    private boolean isDone = false;
    private Cache<Object, Object> cache = null;

    /**
     * Constructor. Build a cache.
     * lock is an Object for synchronization
     */
    @Inject
    InMemoryTask() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(100L)
                .expireAfterAccess(10, TimeUnit.HOURS)
                .concurrencyLevel(4)
                .build();
    }

    /**
     * Wait until receiving a signal.
     * TODO notify this and set isDone to be true to wake up
     */
    @Override
    public byte[] call(byte[] arg0) throws Exception {
        final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

        final String message = "Done";
        writeCache("/tmp");
        while (true) {
            synchronized (this) {
                this.wait();
                if (this.isDone)
                    break;
            }
        }
        return codec.encode(message);
    }

    /**
     * Write files information to cache
     */
    private void writeCache(String path) {
        File files = new File(path);
        Map<String, String> mySortedMap = new TreeMap<String, String>();
        LOG.info("path : " + files.isDirectory());
        if (files.isDirectory()) {
            File[] fileList = files.listFiles();
            for (File child : fileList) {
                mySortedMap.put(child.getName(), child.getPath());
            }
        } else {
            mySortedMap.put(files.getName(), files.getPath());
        }
        cache.putAll(mySortedMap);
        LOG.info("size:" + cache.size() + "\t");
    }
}