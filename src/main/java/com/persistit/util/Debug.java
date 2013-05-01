/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal helper methods used to help verify code correctness.
 * 
 * @author peter
 * 
 */
public class Debug {

    public final static boolean ENABLED = false;

    public final static boolean VERIFY_PAGES = false;

    public final static Random RANDOM = new Random(123);

    private final static AtomicLong PAUSES = new AtomicLong();

    public interface Dbg {
        void t(boolean b);
    }

    private static class Null implements Dbg {
        @Override
        public void t(final boolean b) {
        }
    }

    private static class Assert implements Dbg {
        private final String _name;

        private Assert(final String name) {
            _name = name;
        }

        @Override
        public void t(final boolean b) {
            if (!b) {
                logDebugMessage(_name);
                setSuspended(true);
                //
                // Put a breakpoint on the next statement.
                //
                setSuspended(false); // <-- BREAKPOINT HERE
            }
        }
    }

    public static Dbg $assert0 = ENABLED ? new Assert("assert0") : new Null();
    public static Dbg $assert1 = new Assert("assert1");

    private static int _suspendedCount;

    private static ArrayList<Thread> _brokenThreads = new ArrayList<Thread>();

    private static long _startTime;

    public static void setStartTime(final long startTime) {
        _startTime = startTime;
    }

    public static long elapsedTime() {
        return now() - _startTime;
    }

    public static long now() {
        return System.currentTimeMillis();
    }

    private static void logDebugMessage(final String msg) {
        final RuntimeException exception = new RuntimeException();
        exception.fillInStackTrace();
        final String s = asString(exception).replace('\r', ' ');
        final StringTokenizer st = new StringTokenizer(s, "\n");
        final StringBuilder sb = new StringBuilder(msg);
        sb.append(Util.NEW_LINE);
        while (st.hasMoreTokens()) {
            sb.append("    ");
            sb.append(st.nextToken());
            sb.append(Util.NEW_LINE);
        }
        System.err.println("Debug " + sb.toString());
    }

    public static String trace(final int from, final int to) {
        return " " + Thread.currentThread().getName() + " {" + Debug.callStack(from + 2, to + 2) + "}";
    }

    public static String callStack(final int from, final int to) {
        final RuntimeException exception = new RuntimeException();
        exception.fillInStackTrace();
        final StackTraceElement[] elements = exception.getStackTrace();
        final int a = Math.max(0, from);
        final int b = Math.min(to, elements.length);
        final StringBuilder sb = new StringBuilder();
        for (int index = b; index >= a; index--) {
            final StackTraceElement t = exception.getStackTrace()[index];
            if (index != b) {
                sb.append("->");
            }
            sb.append(t.getClassName());
            sb.append('#');
            sb.append(t.getMethodName());
            sb.append('[');
            sb.append(t.getLineNumber());
            sb.append("]");
        }
        return sb.toString();
    }

    /**
     * Set the suspend flag so that callers to the suspend method either do or
     * do not suspend.
     * 
     * @param b
     */
    synchronized static void setSuspended(final boolean b) {
        if (b) {
            _suspendedCount++;
            _brokenThreads.add(Thread.currentThread());
        } else {
            _suspendedCount--;
            _brokenThreads.remove(Thread.currentThread());
            if (_suspendedCount == 0) {
                $assert1.t(_brokenThreads.size() == _suspendedCount);
            }
        }
    }

    /**
     * @return The state of the suspend flag.
     */
    synchronized static boolean isSuspended() {
        return _suspendedCount > 0;
    }

    /**
     * Assert this method invocation anywhere you want to suspend a thread. For
     * example, add this to cause execution to be suspended:
     * 
     * assert(Debug.suspend());
     * 
     * This method always returns true so there will never be an AssertionError
     * thrown.
     * 
     * @return <i>true</i>
     */
    public static boolean suspend() {
        if (ENABLED) {
            // Never suspend the AWT thread when. The AWT thread is now
            // a daemon thread when running the diagnostic GUI utility.
            //
            long time = -1;
            while (isSuspended() && !Thread.currentThread().isDaemon()) {
                if (time < 0)
                    time = elapsedTime();
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ie) {
                }
            }
        }
        return true;
    }

    public static String asString(final Throwable t) {
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Debugging aid: code can invoke this method to introduce a pause.
     * 
     * @param probability
     *            Probability of pausing: 0 - 1.0f
     * @param millis
     *            time interval in milliseconds
     */
    public static void debugPause(final float probability, final long millis) {
        if (RANDOM.nextInt(1000000000) < (int) (1000000000f * probability)) {
            try {
                Thread.sleep(millis);
                PAUSES.incrementAndGet();
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
