/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.unit;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class ConcurrentUtil {
    public static abstract class ThrowingRunnable {
        public abstract void run() throws Throwable;
    }

    public static Thread createThread(final String name, final ThrowingRunnable runnable) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }, name);
        return t;
    }

    /**
     * Start and join on all given threads. Wait on each thread, individually,
     * for <code>timeout</code> milliseconds. The {@link Thread#join(long)}
     * method is used for this (<code>0</code> means indefinite).
     *
     * @param timeout How long to join on each thread for.
     * @param threads Threads to start and join.
     *
     * @return A map with an entry for each thread that had an unhandled
     * exception or did not complete in the allotted time. This map will be
     * empty if all threads completed successfully.
     */
    public static Map<Thread,Throwable> startAndJoin(long timeout, Thread... threads) {
        final Map<Thread,Throwable> throwableMap = Collections.synchronizedMap(new TreeMap<Thread,Throwable>());

        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                throwableMap.put(t, e);
            }
        };

        for (Thread t : threads) {
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }

        for (Thread t : threads) {
            Throwable error = null;
            try {
                t.join(timeout);
                if (t.isAlive()) {
                    error = new AssertionError("Thread did not complete in timeout: " + timeout);
                }
            } catch (InterruptedException e) {
                error = e;
            }

            if (error != null) {
                throwableMap.put(t, error);
            }
        }

        return throwableMap;
    }

    /**
     * Call {@link #startAndJoin(long, Thread...)} with the given parameters.
     * Additionally, assert that no thread had any unhandled exceptions or
     * timeouts.
     *
     * @param timeout How long to join on each thread for.
     * @param threads Threads to start and join.
     */
    public static void startAndJoinAssertSuccess(long timeout, Thread... threads) {
        Map<Thread,Throwable> errors = startAndJoin(timeout, threads);
        assertEquals("All threads completed successfully", "{}", errors.toString());
    }
}
