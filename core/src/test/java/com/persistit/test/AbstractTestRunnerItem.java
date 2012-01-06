/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.test;

import java.io.PrintStream;

import javax.swing.text.Document;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

/**
 * Test classes must extend this class. The subclass must implement
 * executeTest(), and must post the result of the test to the _result field. The
 * subclass may optionally adjust the _status field and/or the repetitionCount
 * and _progressCount fields. The subclass must stop any lengthy loop iteration
 * in the event _forceStop is set to true.
 */
public abstract class AbstractTestRunnerItem {

    protected String[] _args = new String[0];

    protected Persistit _persistit;

    protected TestUnit _unit;
    /**
     * Subclass posts Result to this
     */
    protected TestResult _result = null;
    /**
     * Subclass should
     */
    protected String _name = getTestName() + ":" + getName();
    protected boolean _started = false;
    protected long _startTime;
    protected boolean _finished = false;
    protected long _finishTime;
    protected boolean _forceStop = false;
    protected String _status = "not started";

    protected int _threadIndex;

    protected boolean _verbose;

    PrintStream _out = System.out;
    PrintStream _err = System.err;
    Document _document;
    String _lastLoggedStatus;

    StringBuilder _sb = new StringBuilder();

    protected int _dotGranularity = 1000;

    private boolean _persistitInitialized;

    public void setUp() throws Exception {
        setUp(true);
    }

    public void setUp(final boolean cleanup) throws Exception {
        if (_persistit == null) {
            _persistit = new Persistit();
            _persistit.initialize(UnitTestProperties.getProperties(cleanup));
            _persistitInitialized = true;
        }
    }

    public void tearDown() throws Exception {
        if (_persistitInitialized) {
            _persistit.close();
            _persistit = null;
            _persistitInitialized = true;
        }
    }

    protected abstract void executeTest() throws Exception;

    protected abstract String shortDescription();

    protected abstract String longDescription();

    protected abstract String getProgressString();

    protected abstract double getProgress();

    protected void setPersistit(final Persistit persistit) {
        _persistit = persistit;
    }

    protected Persistit getPersistit() {
        return _persistit;
    }

    protected void setVerbose(final boolean v) {
        _verbose = v;
    }

    protected boolean isVerbose() {
        return _verbose;
    }

    public String status() {
        if (isPassed()) {
            return "PASSED (" + (_finishTime - _startTime) + "ms)";
        } else if (isFailed()) {
            return "FAILED";
        } else if (isStopped()) {
            return "STOPPED";
        } else if (isStarted()) {
            return "RUNNING";
        } else {
            return "NOT STARTED";
        }
    }

    void initialize(final int index) {
        _result = null;
        _started = false;
        _finished = false;
        _forceStop = false;
        _threadIndex = index;
        _status = "not started";
    }

    public void runIt() {
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException ie) {
        }
        _started = true;
        _startTime = TestRunner.ts();

        println("Starting unit=" + getUnitName() + " test=" + getTestName() + " " + getName() + " at ts=" + _startTime);
        verboseln();
        try {
            setUp();
            executeTest();
        } catch (final Throwable t) {
            if ((t instanceof RuntimeException) && "STOPPED".equals(t.getMessage())) {
                println();
                println("STOPPED");
                if (_result == null) {
                    _result = new TestResult(false, t);
                }
            } else {
                if ((_result == null) || _result._passed) {
                    _result = new TestResult(false, t);
                }
            }
        } finally {
            try {
                tearDown();
            } catch (final Throwable t) {
                if ((_result == null) || _result._passed) {
                    _result = new TestResult(false, t);
                }
            }
        }
        _finishTime = TestRunner.ts();
        println("Finished unit=" + getUnitName() + " test=" + getTestName() + " " + getName() + " at ts=" + _finishTime
                + " - elapsed=" + (_finishTime - _startTime) + " - "
                + (_result == null ? "PASSED" : _result.toString()));
        verboseln();
        if (_result != null) {
            if (!_result._passed) {
                _result.logFailure();
            }
        }
        _finished = true;
    }

    protected synchronized TestResult getResult() {
        return _result;
    }

    protected synchronized boolean isStarted() {
        return _started;
    }

    protected synchronized boolean isFinished() {
        return _finished;
    }

    protected synchronized String getStatus() {
        return _status;
    }

    protected synchronized void forceStop() {
        _forceStop = true;
    }

    protected synchronized boolean isStopped() {
        return _forceStop;
    }

    protected synchronized boolean isPassed() {
        if (isFinished()) {
            return (_result == null) || _result._passed;
        }
        return false;
    }

    protected synchronized boolean isFailed() {
        if (isFinished()) {
            return (_result != null) && !_result._passed;
        }
        return false;
    }

    protected void runStandalone(final String[] args) {
        _unit = new TestUnit("Standalone", 0);
        TestRunner._verbose = true;
        println();
        println(getClass().getSimpleName() + ": " + shortDescription());
        println();
        println(longDescription());
        println();
        _args = args;
        try {
            setUp();
            executeTest();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                tearDown();
            } catch (Exception e) {
                if (!isFailed()) {
                    _result = new TestResult(false, e);
                }
            }
            try {
                _persistit.close();
            } catch (PersistitException e) {
                if (!isFailed()) {
                    _result = new TestResult(false, e);
                }
            }
        }
        println();
        println(getClass().getSimpleName() + ": " + status());
        if (isFailed()) {
            _result.logFailure();
            System.exit(1);
        }
        println();
    }

    @Override
    public String toString() {
        return _name;
    }

    protected String getTestName() {
        final String s = getClass().getName();
        final int p = s.lastIndexOf('.');
        return s.substring(p + 1);
    }

    protected String getUnitName() {
        return _unit._name;
    }

    protected PrintStream getOutputStream() {
        return _out;
    }

    protected void setOutputStream(final PrintStream ps) {
        _out = ps;
    }

    protected void setErrorStream(final PrintStream ps) {
        _err = ps;
    }

    protected PrintStream getErrorStream() {
        return _err;
    }

    protected void setDocument(final Document document) {
        _document = document;
    }

    protected Document getDocument() {
        return _document;
    }

    protected String tsString() {
        long time = TestRunner.ts();
        _sb.setLength(0);
        _sb.append("         ");
        for (int i = 8; --i >= 0;) {
            final char ch = (char) ((time % 10) + '0');
            _sb.setCharAt(i, ch);
            time /= 10;
            if (time == 0) {
                break;
            }
        }
        return _sb.toString();
    }

    protected void println() {
        _out.println();
        _out.flush();
    }

    protected void println(final Object o) {
        _out.println(o);
        _out.flush();
    }

    protected void print(final Object o) {
        _out.print(o);
        _out.flush();
    }

    protected void printf(final String format, final Object... args) {
        println(String.format(format, args));
    }

    protected void printStackTrace(final Throwable t) {
        t.printStackTrace(_err);
    }

    protected void verbosef(final String format, final Object... args) {
        if (isVerbose()) {
            println(String.format(format, args));
        }
    }

    protected void verbose(final Object o) {
        if (isVerbose()) {
            _out.print(o);
            _out.flush();
        }
    }

    protected void verboseln() {
        if (isVerbose()) {
            _out.println();
            _out.flush();
        }
    }

    protected void verboseln(final Object o) {
        if (isVerbose()) {
            _out.println(o);
            _out.flush();
        }
    }

    protected void describeTest(final String m) {
        if (isVerbose()) {
            print(m);
            print(": ");
            for (int i = m.length(); i < 52; i++) {
                print(" ");
            }
        }
    }

    protected void fail(final Object o) {
        println(o);
    }

    public String getName() {
        return Thread.currentThread().getName();
    }
}