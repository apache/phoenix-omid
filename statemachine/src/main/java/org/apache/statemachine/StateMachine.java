/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.statemachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);

    private static final String HANDLER_METHOD_NAME = "handleEvent";

    private static final ConcurrentHashMap<Class<?>, ConcurrentHashMap<Class<?>, Method>> stateCaches;

    static {
        stateCaches = new ConcurrentHashMap<>();
    }

    public abstract static class State {

        protected final Fsm fsm;
        private final ConcurrentHashMap<Class<?>, Method> handlerCache;

        public State(Fsm fsm) {
            this.fsm = fsm;

            ConcurrentHashMap<Class<?>, Method> handlerCache = stateCaches.get(getClass());
            if (handlerCache == null) {
                handlerCache = new ConcurrentHashMap<>();
                ConcurrentHashMap<Class<?>, Method> old = stateCaches.putIfAbsent(getClass(), handlerCache);
                if (old != null) {
                    handlerCache = old;
                }
            }
            this.handlerCache = handlerCache;
        }

        private Method findHandlerInternal(Class<?> state, Class<?> exception) throws NoSuchMethodException {
            Method[] methods = state.getMethods();
            List<Method> candidates = new ArrayList<>();
            for (Method m : methods) {
                if (m.getName().equals(HANDLER_METHOD_NAME)
                        && State.class.isAssignableFrom(m.getReturnType())
                        && m.getGenericParameterTypes().length == 1) {
                    candidates.add(m);
                }
            }

            Method best = null;
            for (Method m : candidates) {
                if (m.getParameterTypes()[0].isAssignableFrom(exception)) {
                    if (best == null) {
                        best = m;
                    } else if (best.getParameterTypes()[0]
                            .isAssignableFrom(m.getParameterTypes()[0])) {
                        best = m;
                    }
                }
            }
            if (best != null) {
                best.setAccessible(true);
                return best;
            }
            throw new NoSuchMethodException("Handler doesn't exist");
        }

        private Method findHandler(Class<?> event) throws NoSuchMethodException {
            Method method = handlerCache.get(event);
            if (method == null) {
                method = findHandlerInternal(getClass(), event);
                Method m2 = handlerCache.putIfAbsent(event, method);
                if (m2 != null) {
                    method = m2;
                }
            }
            return method;
        }

        State dispatch(Event event) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            return (State) findHandler(event.getClass()).invoke(this, event);
        }

    }

    public interface Event {
    }

    public interface Fsm {
        Fsm newChildFsm();

        void setInitState(State initState);

        void sendEvent(Event event);

        Future<?> sendEvent(Event event, long delay, TimeUnit unit);

        void deferEvent(DeferrableEvent deferrableEvent);
    }

    public static class FsmImpl implements Fsm {
        ScheduledExecutorService executor;
        private State state;
        private Queue<DeferrableEvent> deferred;

        public FsmImpl(ScheduledExecutorService executor) {
            this.executor = executor;
            state = null;
            deferred = new ArrayDeque<>();
        }

        private void errorDeferredEvents(Throwable exception) {
            Queue<DeferrableEvent> oldDeferred = deferred;
            deferred = new ArrayDeque<>();

            for (DeferrableEvent e : oldDeferred) {
                e.error(new IllegalStateException(exception));
            }
        }

        @Override
        public Fsm newChildFsm() {
            return new FsmImpl(executor);
        }

        @Override
        public void setInitState(State initState) {
            assert (state == null);
            state = initState;
        }

        public State getState() {
            return state;
        }

        void setState(final State curState, final State newState) {
            if (curState != state) {
                LOG.error("FSM-{}: Tried to transition from {} to {}, but current state is {}",
                          getFsmId(), state, newState, curState);
                throw new IllegalArgumentException();
            }
            state = newState;

            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: State transition {} -> {}", getFsmId(), curState, newState);
            }
        }

        boolean processEvent(Event event) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: Received event {}@{} in state {}@{}",
                          getFsmId(), event.getClass().getSimpleName(),
                          System.identityHashCode(event),
                          state.getClass().getSimpleName(),
                          System.identityHashCode(state));
            }
            try {
                State newState = state.dispatch(event);

                if (newState != state) {
                    setState(state, newState);
                    return true;
                }
            } catch (Throwable t) {
                LOG.error("Caught throwable while handling event", t);
                errorDeferredEvents(t);
            }
            return false;
        }

        class FSMRunnable implements Runnable {
            final Event event;

            FSMRunnable(Event event) {
                this.event = event;
            }

            @Override
            public void run() {
                boolean stateChanged = processEvent(event);
                while (stateChanged) {
                    stateChanged = false;
                    Queue<DeferrableEvent> prevDeferred = deferred;
                    deferred = new ArrayDeque<>();
                    for (DeferrableEvent d : prevDeferred) {
                        if (stateChanged) {
                            deferred.add(d);
                        } else if (processEvent(d)) {
                            stateChanged = true;
                        }
                    }
                }
            }
        }

        @Override
        public void sendEvent(final Event event) {
            executor.submit(new FSMRunnable(event));
        }

        @Override
        public Future<?> sendEvent(final Event event, final long delay, final TimeUnit unit) {
            return executor.schedule(new FSMRunnable(event), delay, unit);
        }

        @Override
        public void deferEvent(DeferrableEvent deferrableEvent) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: deferred {}@{}", getFsmId(),
                    deferrableEvent.getClass().getSimpleName(),
                    System.identityHashCode(deferrableEvent));
            }
            deferred.add(deferrableEvent);
        }

        int getFsmId() {
            return System.identityHashCode(this);
        }

    }

    public interface DeferrableEvent extends Event {
        void error(Throwable exception);
    }

}
