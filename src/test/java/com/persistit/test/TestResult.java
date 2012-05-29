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

import java.util.StringTokenizer;

import com.persistit.util.Debug;

public class TestResult {
    boolean _passed;
    String _message;
    Throwable _throwable;
    TestResult[] _results;

    long _timestamp = TestRunner.ts();
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
        _message = _throwable.toString();
    }

    public TestResult(final boolean passed, final TestResult[] results) {
        _passed = passed;
        _results = results;
    }

    protected void logFailure() {
        logFailure(1);
    }

    protected void logFailure(final int depth) {
        if (_results != null) {
            int failCount = 0;

            for (int index = 0; index < _results.length; index++) {
                final TestResult result1 = _results[index];
                if ((result1 != null) && !result1._passed) {
                    failCount++;
                }
            }

            TestRunner.logMessage("Multithreaded Test Failure: " + failCount + " threads failed", depth);

            for (int index = 0; index < _results.length; index++) {
                final TestResult result1 = _results[index];
                if ((result1 != null) && !result1._passed) {
                    result1.logFailure(depth + 1);
                }
            }
        } else if (_throwable != null) {
            TestRunner.logMessage(_throwable.toString(), depth);
            final String s = Debug.asString(_throwable).replace('\r', ' ');
            final StringTokenizer st = new StringTokenizer(s, "\n");
            while (st.hasMoreTokens()) {
                TestRunner.logMessage(st.nextToken(), depth + 1);
                if (!TestRunner._verbose) {
                    break;
                }
            }
        } else if (_message != null) {
            TestRunner.logMessage(_message, depth);
        } else {
            TestRunner.logMessage("Unspecified failure", depth);
        }
    }

    @Override
    public String toString() {
        if (_passed) {
            return "PASSED: " + _message;
        } else {
            if (_results != null) {
                return "Multithreaded test FAILED";
            } else if (_throwable != null) {
                return "FAILED WITH EXCEPTION\r\n" + Debug.asString(_throwable);
            } else if (_message != null) {
                return "FAILED: " + _message;
            }
            return "FAILED";
        }
    }
}

