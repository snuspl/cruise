package edu.snu.reef.flexion.core;


import org.apache.reef.tang.annotations.Name;

import javax.inject.Inject;
import java.util.HashMap;

public final class KeyValueStore {

    private final HashMap<Class<? extends Name>, Object> hashMap;

    @Inject
    public KeyValueStore(){
        hashMap = new HashMap<>();
    }

    public <T> void put(Class<? extends Name<T>> name, T value) {
        hashMap.put(name, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Class<? extends Name<T>> name) {
        return (T) hashMap.get(name);
    }
}
