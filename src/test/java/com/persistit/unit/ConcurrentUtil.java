/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.unit;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper methods to create, start, join and check error status of test threads.
 * The key element is a Map<Thread, Throwable> the can be checked after threads
 * run for unhandled exceptions. The {@link #assertSuccess(Map)} method is
 * called in the main thread to aggregate and report all exceptions that
 * occurred in other threads.
 */
public class ConcurrentUtil {

    /**
     * An implementation of {@link Thread.UncaughtExceptionHandler} which
     * records any uncaught errors or exceptions in a map. A test case can pass
     * the map to the {@link ConcurrentUtil#assertSuccess(Map)} method verify
     * that no exceptions or errors were caught on test threads.
     * 
     */
    public static class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        final Map<Thread, Throwable> throwableMap = Collections.synchronizedMap(new HashMap<Thread, Throwable>());

        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            throwableMap.put(t, e);
        }

        public Map<Thread, Throwable> getThrowableMap() {
            return throwableMap;
        }
    }

    /**
     * A version of Runnable in which the #run method throws Exception.
     */
    public static abstract class ThrowingRunnable {
        public abstract void run() throws Throwable;
    }

    /**
     * Create a named thread from a ThrowableRunnable.
     * 
     * @param name
     * @param runnable
     * @return
     */
    public static Thread createThread(final String name, final ThrowingRunnable runnable) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (final Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }, name);
    }

    /**
     * Start all given threads. Return a map on which unhandled exceptions will
     * be reported.
     * 
     * @param threads
     *            Threads to start.
     * 
     * @return A map with an entry for each thread that had an unhandled
     *         exception or did not complete in the allotted time. This map will
     *         be empty if all threads completed successfully.
     */
    public static Map<Thread, Throwable> start(final Thread... threads) {
        final UncaughtExceptionHandler handler = new UncaughtExceptionHandler();
        start(handler, threads);
        return handler.getThrowableMap();
    }

    /**
     * Start all given threads with the supplied UncaughtExceptionHandler. The
     * handler will record any uncaught exceptions or errors in a map associated
     * with the handler.
     * 
     * @param handler
     * @param threads
     */
    public static void start(final UncaughtExceptionHandler handler, final Thread... threads) {
        for (final Thread t : threads) {
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }

    }

    /**
     * Wait on each thread, individually, for <code>timeout</code> milliseconds.
     * The {@link Thread#join(long)} method is used for this (<code>0</code>
     * means indefinite). Add an Exception to the error map for any thread that
     * did not end within its timeout.
     * 
     * @param timeout
     * @param throwableMap
     * @param threads
     */
    public static void join(final long timeout, final Map<Thread, Throwable> throwableMap, final Thread... threads) {
        for (final Thread t : threads) {
            Throwable error = null;
            try {
                t.join(timeout);
                if (t.isAlive()) {
                    error = new AssertionError("Thread did not complete in timeout: " + timeout);
                }
            } catch (final InterruptedException e) {
                error = e;
            }
            if (error != null) {
                throwableMap.put(t, error);
            }
        }
    }

    /**
     * Assert that no thread had any unhandled exceptions or timeouts.
     * 
     * @param throwableMap
     *            map in which threads accumulated any unhandled Exceptions
     */
    public static void assertSuccess(final Map<Thread, Throwable> throwableMap) {
        String description = "";
        for (final Map.Entry<Thread, Throwable> entry : throwableMap.entrySet()) {
            description += " " + entry.getKey().getName() + "=" + entry.getValue().toString();
        }
        assertEquals("All threads completed successfully", "{}", "{" + description + "}");
    }

    /**
     * Call {@link #start(Thread...)} for all threads and then
     * {@link #join(long, Map, Thread...)} for all threads.
     * 
     * @param timeout
     *            How long to join on each thread for.
     * @param threads
     *            Threads to start and join.
     * 
     * @return A map with an entry for each thread that had an unhandled
     *         exception or did not complete in the allotted time. This map will
     *         be empty if all threads completed successfully.
     */
    public static Map<Thread, Throwable> startAndJoin(final long timeout, final Thread... threads) {
        final Map<Thread, Throwable> throwableMap = start(threads);
        join(timeout, throwableMap, threads);
        return throwableMap;
    }

    /**
     * Call {@link #startAndJoin(long, Thread...)} with the given parameters.
     * Additionally, assert that no thread had any unhandled exceptions or
     * timeouts.
     * 
     * @param timeout
     *            How long to join on each thread for.
     * @param threads
     *            Threads to start and join.
     */
    public static void startAndJoinAssertSuccess(final long timeout, final Thread... threads) {
        final Map<Thread, Throwable> throwableMap = startAndJoin(timeout, threads);
        assertSuccess(throwableMap);
    }
}
