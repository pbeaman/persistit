/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.stress;

import com.persistit.Persistit;

/**
 * Test classes must extend this class. The subclass must implement
 * executeTest(), and must post the result of the test to the _result field. The
 * subclass may optionally adjust the _status field and/or the repetitionCount
 * and _progressCount fields. The subclass must stop any lengthy loop iteration
 * in the event _forceStop is set to true.
 */
public abstract class AbstractStressTest implements Runnable {

    /**
     * Subclass posts Result to this
     */
    protected volatile TestResult _result = null;
    /**
     * Subclass should
     */
    protected volatile long _startTime;
    protected volatile long _endTime;
    protected volatile boolean _started = false;
    protected volatile boolean _finished = false;
    protected volatile boolean _forceStop = false;
    protected volatile boolean _untilStopped = false;
    protected String[] _args;
    protected int _threadIndex;
    protected boolean _verbose;
    protected String _threadName = "<not started>";

    private volatile long _totalWorkDone;

    Persistit _persistit;

    protected Persistit getPersistit() {
        return _persistit;
    }

    protected void setPersistit(Persistit db) {
        _persistit = db;
    }

    protected abstract void executeTest() throws Exception;

    protected AbstractStressTest(final String argsString) {
        if (argsString == null) {
            _args = new String[0];
        } else {
            _args = argsString.split(" ");
        }
    }

    void initialize(final int index) {
        _result = null;
        _started = false;
        _finished = false;
        _forceStop = false;
        _threadIndex = index;
    }

    public void run() {
        _threadName = Thread.currentThread().getName();
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException ie) {
        }
        _started = true;

        try {
            setUp();
            _startTime = System.nanoTime();
            executeTest();
            _endTime = System.nanoTime();
            tearDown();

        } catch (final Throwable t) {
            if ((t instanceof RuntimeException) && "STOPPED".equals(t.getMessage())) {
                pass();
            } else {
                fail(t);
            }
        }
        if (isFailed()) {
            System.err.printf("\n%s: %s\n", toString(), _result.toString());
        }
        _finished = true;
    }

    protected void pass() {
        if (_result == null) {
            _result = new TestResult(true);
        }
    }

    protected void pass(final String message) {
        if (_result == null) {
            _result = new TestResult(true, message);
        }
    }

    protected void fail(final String message) {
        if (!isFailed()) {
            _result = new TestResult(false, message);
        }
        forceStop();
        System.err.printf("\n%s: %s\n", this, _result);
    }

    protected void fail(final Throwable throwable) {
        if (!isFailed()) {
            _result = new TestResult(false, throwable);
        }
        forceStop();
        long elapsed = (System.nanoTime() - _startTime) / AbstractSuite.NS_PER_S;
        System.err.printf("\n%s at %,d seconds: %s\n", this, elapsed, _result);
    }

    protected TestResult getResult() {
        return _result;
    }

    protected boolean isStarted() {
        return _started;
    }

    protected boolean isFinished() {
        return _finished;
    }

    protected void forceStop() {
        _forceStop = true;
    }

    protected boolean isStopped() {
        return _forceStop;
    }

    protected boolean isPassed() {
        if (isFinished()) {
            return (_result == null) || _result._passed;
        }
        return false;
    }

    protected boolean isFailed() {
        if (isFinished()) {
            return (_result != null) && !_result._passed;
        }
        return false;
    }

    @Override
    public String toString() {
        return getTestName() + "[" + getThreadName() + "]";
    }

    protected String getTestName() {
        return getClass().getSimpleName();
    }

    protected void setUp() throws Exception {

    }

    protected void tearDown() throws Exception {

    }

    public String getThreadName() {
        return _threadName;
    }

    public void setUntilStopped(final boolean untilStopped) {
        _untilStopped = untilStopped;
    }

    public boolean isUntilStopped() {
        return _untilStopped;
    }

    protected void addWork(final long work) {
        _totalWorkDone += work;
    }

    public long getTotalWorkDone() {
        return _totalWorkDone;
    }

}
