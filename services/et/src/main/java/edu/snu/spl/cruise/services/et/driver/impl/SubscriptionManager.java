/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.spl.cruise.services.et.driver.impl;

import edu.snu.spl.cruise.services.et.driver.api.MessageSender;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that manages subscription of ownership updates of tables.
 * It broadcasts updates to registered subscribers.
 * When registering new subscribers dynamically, it accumulates intermittent updates and
 * delivers later when the subscribers are ready to get updates.
 */
public final class SubscriptionManager {
  private static final Logger LOG = Logger.getLogger(SubscriptionManager.class.getName());

  /**
   * A mapping between table id and a set of ids of corresponding subscribers.
   */
  private final Map<String, Set<String>> subscribersPerTable = new ConcurrentHashMap<>();

  /**
   * Data structures for managing subscription init.
   */
  private final Map<Integer, SubscriptionInit> opIdToSubscriptionInit = new ConcurrentHashMap<>();
  private final Map<String, Set<SubscriptionInit>> tableIdToSubscriptionInits = new ConcurrentHashMap<>();

  private final AtomicInteger initOpIdCount = new AtomicInteger(0);

  private final MessageSender msgSender;
  private final String driverId;

  @Inject
  private SubscriptionManager(final MessageSender msgSender,
                              @Parameter(DriverIdentifier.class) final String driverId) {
    this.msgSender = msgSender;
    this.driverId = driverId;
  }

  /**
   * Registers a subscriber for the update of partition status of a table whose id is {@code tableId}.
   * Whenever a block has been moved, the executor with {@code executorId} will be notified.
   * @param tableId a table id
   * @param executorId a executor id
   */
  synchronized void registerSubscription(final String tableId, final String executorId) {
    subscribersPerTable.compute(tableId, (tId, executorIdSet) -> {
      final Set<String> value = executorIdSet == null ? Collections.synchronizedSet(new HashSet<>()) : executorIdSet;
      if (!value.add(executorId)) {
        throw new RuntimeException(String.format("Table %s already has subscriber %s", tId, executorId));
      }

      return value;
    });
  }

  /**
   * Unregisters a subscriber for the update of partition status of a table whose id is {@code tableId}.
   * @param tableId a table id
   * @param executorId a executor id
   */
  synchronized void unregisterSubscription(final String tableId, final String executorId) {
    subscribersPerTable.compute(tableId, (tId, executorIdSet) -> {
      if (executorIdSet == null) {
        throw new RuntimeException(String.format("Table %s does not exist", tId));
      }
      if (!executorIdSet.remove(executorId)) {
        throw new RuntimeException(String.format("Table %s does not have subscriber %s", tId, executorId));
      }

      return executorIdSet.isEmpty() ? null : executorIdSet;
    });
  }

  /**
   * Unregisters all subscribers of a table with id {@code tableId}.
   * @param tableId a table id
   * @return a set of unregistered executor ids
   */
  synchronized Set<String> unregisterSubscribers(final String tableId) {
    return subscribersPerTable.remove(tableId);
  }

  /**
   * @param tableId a table id
   * @return a set of executor ids that subscribe the table
   */
  Set<String> getSubscribers(final String tableId) {
    final Set<String> subscribers = subscribersPerTable.get(tableId);
    return subscribers == null ? new HashSet<>() : new HashSet<>(subscribers);
  }

  /**
   * Broadcast to ownership update to subscribers.
   * @param tableId a table id
   * @param blockId a block id
   * @param senderId a sender id
   * @param receiverId a receiver id
   */
  synchronized void broadcastUpdate(final String tableId, final int blockId,
                                    final String senderId, final String receiverId) {

    final Set<SubscriptionInit> subscriptionInits = tableIdToSubscriptionInits.get(tableId);
    if (subscriptionInits != null) {
      subscriptionInits.forEach(subscriptionInit -> subscriptionInit.accumulateUpdate(blockId, receiverId));
    }

    final Set<String> subscribers = new HashSet<>(subscribersPerTable.get(tableId));
    subscribers.remove(senderId);
    subscribers.remove(receiverId);

    LOG.log(Level.FINE, "Ownership moved. tableId: {0}, blockId: {1}." +
        " Broadcast the ownership update to other subscribers: {2}", new Object[]{tableId, blockId, subscribers});

    subscribers.forEach(executorId ->
        msgSender.sendOwnershipUpdateMsg(executorId, tableId, blockId, senderId, receiverId));
  }

  /**
   * Starts {@link SubscriptionInit}, which accumulates updates for dynamically registering subscribers.
   * @param tableId a table id
   * @param executorIdSet a set of executor ids to be subscribers
   * @return an operation id
   */
  synchronized int startSubscriptionInit(final String tableId, final Set<String> executorIdSet) {
    final int opId = initOpIdCount.getAndIncrement();
    final SubscriptionInit subscriptionInit = new SubscriptionInit(executorIdSet, tableId);

    opIdToSubscriptionInit.put(opId, subscriptionInit);
    tableIdToSubscriptionInits.compute(tableId, (tId, subscriptionInits) -> {
      if (subscriptionInits == null) {
        final Set<SubscriptionInit> newSubscriptionInits = new HashSet<>();
        newSubscriptionInits.add(subscriptionInit);
        return Collections.synchronizedSet(newSubscriptionInits);
      } else {
        subscriptionInits.add(subscriptionInit);
        return subscriptionInits;
      }
    });

    return opId;
  }

  /**
   * Finishes {@link SubscriptionInit} and broadcasts accumulated updates to subscribers.
   * By calling this method, subscription register process is completed.
   * @param opId an operation id issued by {@link #startSubscriptionInit(String, Set)}
   */
  synchronized void finishSubscriptionInit(final int opId) {
    final SubscriptionInit subscriptionInit = opIdToSubscriptionInit.remove(opId);
    if (subscriptionInit == null) {
      throw new RuntimeException();
    }
    tableIdToSubscriptionInits.get(subscriptionInit.tableId).remove(subscriptionInit);

    // complete by registering subscription
    subscriptionInit.preSubscribers.forEach(executorId -> registerSubscription(subscriptionInit.tableId, executorId));

    // broadcast all accumulated updates
    LOG.log(Level.INFO, "Broadcast accumulated updates of {0}: {1} to {2}",
        new Object[]{subscriptionInit.tableId, subscriptionInit.accumulatedUpdates, subscriptionInit.preSubscribers});
    subscriptionInit.accumulatedUpdates.forEach((blockId, ownerId) ->
        subscriptionInit.preSubscribers.forEach(executorId ->
            msgSender.sendOwnershipUpdateMsg(executorId, subscriptionInit.tableId, blockId, driverId, ownerId)));
  }

  /**
   * A class that represents the initialization of subscription.
   * During initializing a specific table to pre-subscribers,
   * it accumulates ownership updates that will be delivered at once after the initialization.
   */
  private class SubscriptionInit {
    private final String tableId;
    private final Map<Integer, String> accumulatedUpdates;
    private final Set<String> preSubscribers;

    SubscriptionInit(final Set<String> preSubscribers, final String tableId) {
      this.tableId = tableId;
      this.accumulatedUpdates = new ConcurrentHashMap<>();
      this.preSubscribers = Collections.unmodifiableSet(preSubscribers);
    }

    void accumulateUpdate(final int blockId, final String newOwnerId) {
      accumulatedUpdates.put(blockId, newOwnerId);
    }
  }
}
