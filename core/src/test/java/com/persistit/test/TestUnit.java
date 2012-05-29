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

import java.util.ArrayList;

import com.persistit.test.TestRunner.Command;

public class TestUnit {
    String _name;
    int _lineNumber;
    ArrayList<Command> _commands = new ArrayList<Command>();
    boolean _passed = true; // Until failure

    public TestUnit(final String name, final int lineNumber) {
        _name = name;
        _lineNumber = lineNumber;
    }

    public TestResult execute() {
        final ArrayList<TestResult> results = new ArrayList<TestResult>();
        final int size = _commands.size();
        for (int index = 0; !TestRunner._stopAll && (index < size); index++) {
            final TestResult result = _commands.get(index).execute();
            if (result != null) {
                results.add(result);
                if (!result._passed) {
                    _passed = false;
                }
            }
        }
        return new TestResult(_passed, results.toArray(TestRunner.EMPTY_RESULT_ARRAY));
    }
}

