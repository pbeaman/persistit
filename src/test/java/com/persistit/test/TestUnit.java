/**
 * 
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
        return new TestResult(_passed,
                (TestResult[]) results.toArray(TestRunner.EMPTY_RESULT_ARRAY));
    }
}