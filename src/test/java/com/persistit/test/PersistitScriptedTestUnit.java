/**
 * 
 */
package com.persistit.test;

import java.util.ArrayList;

import com.persistit.test.ScriptedTestRunner.Command;

public class PersistitScriptedTestUnit {
	String _name;
	int _lineNumber;
	ArrayList _commands = new ArrayList();
	boolean _passed = true; // Until failure

	PersistitScriptedTestUnit(final String name, final int lineNumber) {
		_name = name;
		_lineNumber = lineNumber;
	}

	public PersistitTestResult execute() {
		final ArrayList results = new ArrayList();
		final int size = _commands.size();
		for (int index = 0; !ScriptedTestRunner._stopAll && (index < size); index++) {
			final PersistitTestResult result = ((Command) _commands.get(index))
					.execute();
			if (result != null) {
				results.add(result);
				if (!result._passed) {
					_passed = false;
				}
			}
		}
		return new PersistitTestResult(_passed, (PersistitTestResult[]) results
				.toArray(ScriptedTestRunner.EMPTY_RESULT_ARRAY));
	}
}