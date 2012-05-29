/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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
