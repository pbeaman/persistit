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

/**
 * 
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