/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
