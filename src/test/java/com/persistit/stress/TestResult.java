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

import com.persistit.util.Debug;

public class TestResult {
    String _threadName = Thread.currentThread().getName();
    boolean _passed;
    String _message;
    Throwable _throwable;
    TestResult[] _results;

    long _timestamp = System.nanoTime();
    long _elapsedTime = 0;

    protected TestResult(final boolean passed) {
        _passed = passed;
    }

    public TestResult(final boolean passed, final String message) {
        _passed = passed;
        _message = message;
    }

    public TestResult(final boolean passed, final Throwable throwable) {
        _passed = passed;
        _throwable = throwable;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(_passed ? "PASSED" : "FAILED");
        if (_throwable != null) {
            sb.append(" [" + _threadName + "]");
            sb.append(": ");
            sb.append(Debug.asString(_throwable));
        } else if (_message != null) {
            sb.append(": ");
            sb.append(_message);
        }
        return sb.toString();
    }
}
