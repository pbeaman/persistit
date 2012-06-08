/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
public abstract class AbstractTestRunnerItem implements Runnable {

    /**
     * Subclass posts Result to this
     */
    protected TestResult _result = null;
    /**
     * Subclass should
     */
    protected String _name = getTestName() + ":" + getName();
    protected boolean _started = false;
    protected boolean _finished = false;
    protected boolean _forceStop = false;
    protected String _status = "not started";
    protected String[] _args;
    protected int _threadIndex;
    protected boolean _verbose;


    
    Persistit _persistit;
    
    protected Persistit getPersistit() {
        return _persistit;
    }
    
    protected void setPersistit(Persistit db) {
        _persistit = db;
    }

    protected abstract void executeTest() throws Exception;

    protected abstract String getProgressString();

    protected abstract double getProgress();

    protected AbstractTestRunnerItem(final String argsString) {
        _args = argsString.split(" ");
    }

    void initialize(final int index) {
        _result = null;
        _started = false;
        _finished = false;
        _forceStop = false;
        _threadIndex = index;
        _status = "not started";
    }

    public void run() {
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException ie) {
        }
        _started = true;

        try {
            setUp();
            executeTest();
            tearDown();
            _result = new TestResult(true);
            
        } catch (final Throwable t) {
            if ((t instanceof RuntimeException) && "STOPPED".equals(t.getMessage())) {
                if (_result == null) {
                    _result = new TestResult(true);
                }
            } else {
                if ((_result == null) || _result._passed) {
                    _result = new TestResult(false, t);
                }
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

    @Override
    public String toString() {
        return _name;
    }

    protected String getTestName() {
        final String s = getClass().getName();
        final int p = s.lastIndexOf('.');
        return s.substring(p + 1);
    }

    protected void setUp() throws Exception {

    }

    protected void tearDown() throws Exception {

    }

    public String getName() {
        return Thread.currentThread().getName();
    }
    
}
