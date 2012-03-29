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

import com.persistit.test.AbstractTestRunnerItem;
import com.persistit.test.TestResult;

/**
 * Adapter that allows the TestRunner to run unit tests
 * 
 * @author peter
 * 
 */
public class UnitTestRunner extends AbstractTestRunnerItem {
    boolean _done;
    String _className;
    String[] _args;

    @Override
    public void executeTest() {
        try {
            System.out.println("Running Unit Test " + _className);
            final Class cl = Class.forName(_className);
            final PersistitUnitTestCase testCase = (PersistitUnitTestCase) cl.newInstance();
            testCase.setPersistit(_persistit);
            testCase.runAllTests();
            _done = true;
        } catch (final Exception e) {
            _done = true;
            final Throwable t = e;
            // if (e.getCause() != null) t = e.getCause();
            _result = new TestResult(false, t);
        }
    }

    @Override
    protected String shortDescription() {
        return "Unit Test: " + _className;
    }

    @Override
    protected String longDescription() {
        return shortDescription();
    }

    @Override
    protected String getProgressString() {
        return _done ? "done" : "running";
    }

    @Override
    protected double getProgress() {
        return _done ? 1.0 : 0.0;
    }

    protected void done() {
        _done = true;
    }

}
