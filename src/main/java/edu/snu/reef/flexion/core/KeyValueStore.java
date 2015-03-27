package edu.snu.reef.flexion.core;


import org.apache.reef.tang.annotations.Name;

import javax.inject.Inject;
import java.util.HashMap;

public final class KeyValueStore {

    private final HashMap<Name, Object> hashMap;

    @Inject
    public KeyValueStore(){
        hashMap = new HashMap<>();
    }

    public <T> void put(Name<T> name, T value) {
        hashMap.put(name, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Name<T> name) {
        return (T) hashMap.get(name);
    }
}
