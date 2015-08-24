/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.shuffle.utils;

import org.apache.reef.io.Tuple;

import java.util.*;

/**
 * Finite state machine that can be created with user defined states and transitions.
 */
public final class StateMachine {

  private final Map<String, State> stateMap;
  private State currentState;

  private StateMachine(final Map<String, State> stateMap, final String initialState) {
    this.stateMap = stateMap;
    this.currentState = stateMap.get(initialState);
  }

  /**
   * Check whether current state is same as the expectedCurrentState.
   *
   * @param expectedCurrentState an expected current state
   * @throws RuntimeException if the expectedCurrentState is not same as actual current state
   */
  public synchronized void checkState(final String expectedCurrentState) {
    if (!currentState.name.equals(expectedCurrentState)) {
      final String exceptionMessage = new StringBuilder()
          .append("The expected current state is ")
          .append(expectedCurrentState)
          .append(" but the state is ")
          .append(currentState).append('\n')
          .append(getPossibleTransitionsFromCurrentState())
          .toString();

      throw new RuntimeException(exceptionMessage);
    }
  }

  /**
   * Set current state as a certain state.
   * @param state a state
   * @throws RuntimeException if the state is unknown state, or the transition
   * from the current state to the specified state is illegal
   */
  public synchronized void setState(final String state) {
    if (!stateMap.containsKey(state)) {
      throw new RuntimeException("Unknown state " + state);
    }

    final State toState = stateMap.get(state);
    if (!currentState.isLegalTransition(state)) {
      final String exceptionMessage = new StringBuilder()
          .append("Illegal transition from ")
          .append(currentState)
          .append(" to ")
          .append(toState).append('\n')
          .append(getPossibleTransitionsFromCurrentState())
          .toString();
      throw new RuntimeException(exceptionMessage);
    }

    currentState = toState;
  }

  /**
   * Check the expectedCurrentState and set to a certain state.
   *
   * @param expectedCurrentState an expected state
   * @param state a state
   */
  public synchronized void checkAndSetState(final String expectedCurrentState, final String state) {
    checkState(expectedCurrentState);
    setState(state);
  }

  /**
   * @return the name of the current state.
   */
  public synchronized String getCurrentState() {
    return currentState.name;
  }

  private String getPossibleTransitionsFromCurrentState() {
    final StringBuilder stringBuilder = new StringBuilder()
        .append("Possible transitions from the current state are").append('\n');

    for (final Transition transition : currentState.getAllTransitions()) {
      stringBuilder.append(transition).append('\n');
    }

    return stringBuilder.toString();
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (final State state : stateMap.values()) {
      stringBuilder.append(state).append('\n')
          .append("Possible transitions:").append('\n');
      for (final Transition transition : state.getAllTransitions()) {
        stringBuilder.append(transition).append('\n');
      }
      stringBuilder.append('\n');
    }
    return stringBuilder.toString();
  }

  /**
   * @return a builder of StateMachine
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private static final class State {
    private final String name;
    private final String description;
    private final Map<String, Transition> transitions;

    private State(final String name, final String description) {
      this.name = name;
      this.description = description;
      this.transitions = new HashMap<>();
    }

    private void addTransition(final Transition transition) {
      if (transition.from != this) {
        throw new RuntimeException("An illegal transition " + transition + " was added to " + this);
      }

      transitions.put(transition.to.name, transition);
    }

    private boolean isLegalTransition(final String to) {
      return transitions.containsKey(to);
    }

    private Collection<Transition> getAllTransitions() {
      return transitions.values();
    }

    @Override
    public String toString() {
      return name + "[" + description + "]";
    }
  }

  private static final class Transition {
    private final State from;
    private final State to;
    private final String description;
    private Transition(final State from, final State to, final String description) {
      this.from = from;
      this.to = to;
      this.description = description;
    }

    @Override
    public String toString() {
      return "Transition from " + from + " to " + to + " : " + description;
    }
  }

  /**
   * Builder that build a StateMachine.
   */
  public static final class Builder {
    private final Set<String> stateSet;
    private final Map<String, String> stateDescriptionMap;
    private final Map<String, Set<Tuple<String, String>>> transitionMap;

    private String initialState;

    private Builder() {
      this.stateSet = new HashSet<>();
      this.stateDescriptionMap = new HashMap<>();
      this.transitionMap = new HashMap<>();
    }

    /**
     * Add a state with name and description.
     *
     * @param name name of the state
     * @param description description of the state
     * @return the builder
     * @throws RuntimeException if the state was already added
     */
    public Builder addState(final String name, final String description) {
      if (stateSet.contains(name)) {
        throw new RuntimeException("A state " + name + " was already added");
      }

      stateSet.add(name);
      stateDescriptionMap.put(name, description);
      return this;
    }

    /**
     * @param initialState the initial state for StateMachine
     * @return the builder
     * @throws RuntimeException if the initial state was not added
     */
    public Builder setInitialState(final String initialState) {
      if (!stateSet.contains(initialState)) {
        throw new RuntimeException("A state " + initialState + " should be added first");
      }
      this.initialState = initialState;
      return this;
    }

    /**
     * Add a transition with description.
     *
     * @param from from state name
     * @param to to state name
     * @param description description of the transition
     * @return the builder
     * @throws RuntimeException if either from or to state was not added, or the same transition
     * was already added
     */
    public Builder addTransition(final String from, final String to, final String description) {
      if (!stateSet.contains(from)) {
        throw new RuntimeException("A state " + from + " should be added first");
      }

      if (!stateSet.contains(to)) {
        throw new RuntimeException("A state " + to + " should be added first");
      }

      final Tuple<String, String> transition = new Tuple<>(to, description);

      if (!transitionMap.containsKey(from)) {
        transitionMap.put(from, new HashSet<Tuple<String, String>>());
      }

      if (transitionMap.get(from).contains(transition)) {
        throw new RuntimeException("A transition from " + from + " to " + to + " was already added");
      }

      transitionMap.get(from).add(transition);
      return this;
    }

    /**
     * Build and return the StateMachine.
     *
     * @return the StateMachine
     * @throws RuntimeException if an initial state was not set
     */
    public StateMachine build() {
      if (initialState == null) {
        throw new RuntimeException("An initial state should be set");
      }

      final Map<String, State> stateMap = new HashMap<>();
      for (final String stateName : stateSet) {
        stateMap.put(stateName, new State(stateName, stateDescriptionMap.get(stateName)));
      }

      for (final String stateName : stateSet) {
        final State state = stateMap.get(stateName);
        if (transitionMap.containsKey(stateName)) {
          for (final Tuple<String, String> transition : transitionMap.get(stateName)) {
            state.addTransition(new Transition(state, stateMap.get(transition.getKey()), transition.getValue()));
          }
        }
      }

      return new StateMachine(stateMap, initialState);
    }
  }
}
