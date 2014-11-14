package org.apache.reef.inmemory.common.instrumentation;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Event Implementation
 */
public final class EventImpl implements Event {
  private static final Logger LOG = Logger.getLogger(EventImpl.class.getName());

  private final String group;
  private final String id;
  private long start;
  private long stop;

  EventImpl(final String group, final String id) {
    this.group = group;
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
  public String getGroup() {
    return group;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public long getDuration() {
    return stop - start;
  }

  @Override
  public String toJsonString() {
    final JSONObject json = new JSONObject();
    try {
      json.put("group", group);
      json.put("id", id);
      json.put("duration", getDuration() / 1000);
      return json.toString();
    } catch (JSONException e) {
      LOG.log(Level.WARNING, "Could not parse json for {0}, {1}, {2}",
              new Object[]{group, id, Long.toString(getDuration() / 1000)});
      LOG.log(Level.WARNING, "Could not parse json with", e);
      return null;
    }
  }
}
