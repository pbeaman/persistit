/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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
