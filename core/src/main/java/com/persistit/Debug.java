/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.persistit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

public class Debug {
    public final static boolean ENABLED = false;

    public final static boolean VERIFY_PAGES = false;

    public final static boolean HISTORY_ENABLED = false;

    public final static boolean IOLOG_ENABLED = false;

    private static int _suspendedCount;
    // Lazily instantiated
    private static Random _random;

    private static HashMap _threadClaims = new HashMap();

    private static ArrayList _brokenThreads = new ArrayList();

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

    private static void logDebugMessage(String msg) {
        long now = now();

        DebugException de = new DebugException();
        de.fillInStackTrace();
        String s = LogBase.detailString(de).replace('\r', ' ');
        StringTokenizer st = new StringTokenizer(s, "\n");
        StringBuilder sb = new StringBuilder(msg);
        sb.append(Util.NEW_LINE);
        while (st.hasMoreTokens()) {
            sb.append("    ");
            sb.append(st.nextToken());
            sb.append(Util.NEW_LINE);
        }
        System.err.println("Debug " + sb.toString());
    }

    private static class DebugException extends Exception {
    }

    /**
     * Use this method for a conditional breakpoint that executes at full speed.
     * Set a debugger breakpoint where indicated.
     * 
     * @param condition
     *            <i>true</i> if the breakpoint should be taken
     * @return <i>true</i>
     */
    public static boolean debug(boolean condition) {
        if (!condition)
            return false;
        // Put a breakpoint on this return statement.
        logDebugMessage("debug");
        return true; // <-- BREAKPOINT HERE
    }

    /**
     * Use this method for a conditional breakpoint that executes at full speed.
     * Set a debugger breakpoint where indicated. This method also sets the
     * suspend flag so other threads will be suspended at a suspend point if
     * necessary. (Simplifies debugging because the diagnostic UI still works in
     * this situation.)
     * 
     * @param condition
     *            <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug0(boolean condition) {
        if (condition) {
            logDebugMessage("debug0");
            setSuspended(true);
            long time = elapsedTime();

            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false); // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Use this method for a conditional breakpoint that executes at full speed.
     * Set a debugger breakpoint where indicated. This method also sets the
     * suspend flag so other threads will be suspended at a suspend point if
     * necessary. (Simplifies debugging because the diagnostic UI still works in
     * this situation.)
     * 
     * @param condition
     *            <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug1(boolean condition) {
        if (condition) {
            logDebugMessage("debug1");
            setSuspended(true);
            long time = elapsedTime();
            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false); // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Use this method for a conditional breakpoint that executes at full speed.
     * Set a debugger breakpoint where indicated. This method also sets the
     * suspend flag so other threads will be suspended at a suspend point if
     * necessary. (Simplifies debugging because the diagnostic UI still works in
     * this situation.)
     * 
     * @param condition
     *            <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug2(boolean condition) {
        if (condition) {
            logDebugMessage("debug2");
            setSuspended(true);
            long time = elapsedTime();

            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false); // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Use this method for a conditional breakpoint that executes at full speed.
     * Set a debugger breakpoint where indicated. This method also sets the
     * suspend flag so other threads will be suspended at a suspend point if
     * necessary. (Simplifies debugging because the diagnostic UI still works in
     * this situation.)
     * 
     * @param condition
     *            <i>true</i> if the breakpoint should be taken
     * @return <i>false</i>
     */
    public static boolean debug3(boolean condition) {
        if (condition) {
            logDebugMessage("debug3");
            setSuspended(true);
            long time = elapsedTime();

            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false); // <-- BREAKPOINT HERE
            return true;
        }
        return false;
    }

    /**
     * Replace assert statements with calls to this method in order to take a
     * breakpoint before throwing the AssertionError if the condition is false.
     * 
     * @param condition
     */
    public static void $assert(boolean condition) {
        if (!condition) {
            setSuspended(true);
            logDebugMessage("$assert");
            long time = elapsedTime();

            //
            // Put a breakpoint on the next statement.
            //
            setSuspended(false); // <-- BREAKPOINT HERE
            /* JDK14 */// assert(false);
        }
    }

    /**
     * Invoke this method to sleep briefly on a random basis. This method
     * invokes sleep approximately once per thousand invocations.
     */
    public static void debugPause() {
        debugPause(true);
    }

    /**
     * Invoke this method to sleep briefly on a random basis. This method
     * invokes sleep approximately once per thousand invocations on which the
     * condition is true.
     * 
     * @param condition
     *            <i>true<i> to whether to pause with 0.1% probability
     */
    public static void debugPause(boolean condition) {
        debugPause(condition, 0.001);
    }

    /**
     * Invoke this method to sleep briefly on a random basis. Supply a double
     * value to indicate the probability that should be used.
     * 
     * @param condition
     *            <i>true<i> to whether to pause
     */
    public static void debugPause(boolean condition, double probability) {
        if (_random == null)
            _random = new Random(1000);
        if (condition && probability >= 1.0
                || _random.nextFloat() < probability) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
            }
        }
    }

    /**
     * Invoke this method to perform a System.exit() operation on a random basis
     * according to the supplied probability.
     * 
     * @param probability
     */
    public static void debugExit(double probability) {
        if (_random == null)
            _random = new Random(1000);
        if (probability >= 1.0 || _random.nextFloat() < probability) {
            logDebugMessage("debugExit");
            System.out.println();
            System.out.println("DEBUG EXIT!");
            System.exit(0);
        }
    }

    /**
     * Set the suspend flag so that callers to the suspend method either do or
     * do not suspend.
     * 
     * @param b
     */
    synchronized static void setSuspended(boolean b) {
        if (b) {
            _suspendedCount++;
            _brokenThreads.add(Thread.currentThread());
        } else {
            _suspendedCount--;
            _brokenThreads.remove(Thread.currentThread());
            if (_suspendedCount == 0) {
                $assert(_brokenThreads.size() == _suspendedCount);
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
    static boolean suspend() {
        // Never suspend the AWT thread when. The AWT thread is now
        // a daemon thread when running the diagnostic GUI utility.
        //
        long time = -1;
        while (isSuspended() && !Thread.currentThread().isDaemon()) {
            if (time < 0)
                time = elapsedTime();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
        return true;
    }


}
