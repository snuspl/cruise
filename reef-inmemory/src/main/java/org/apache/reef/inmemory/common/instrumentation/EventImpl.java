package org.apache.reef.inmemory.common.instrumentation;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class EventImpl implements Event {
  private static final Logger LOG = Logger.getLogger(EventImpl.class.getName());

  private final String name;
  private final String id;
  private long start;
  private long stop;

  EventImpl(final String name, final String id) {
    this.name = name;
    this.id = id;
  }

  @Override
  public EventImpl start() {
    this.start = System.nanoTime();
    return this;
  }

  @Override
  public EventImpl stop() {
    this.stop = System.nanoTime();
    return this;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getDuration() {
    return stop - start;
  }

  @Override
  public String toJsonString() {
    final JSONObject json = new JSONObject();
    try {
      json.put("name", name);
      json.put("id", id);
      json.put("duration", getDuration() / 1000);
      return json.toString();
    } catch (JSONException e) {
      LOG.log(Level.WARNING, "Could not parse json for {0}, {1}, {2}",
              new Object[]{name, id, Long.toString(getDuration() / 1000)});
      LOG.log(Level.WARNING, "Could not parse json with", e);
      return null;
    }
  }
}
