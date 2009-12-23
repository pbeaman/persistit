/**
 * 
 */
package com.persistit.test;

import java.util.StringTokenizer;

import com.persistit.LogBase;

public class PersistitTestResult {
	boolean _passed;
	String _message;
	Throwable _throwable;
	PersistitTestResult[] _results;

	long _timestamp = TestRunner.ts();
	long _elapsedTime = 0;

	protected PersistitTestResult(final boolean passed) {
		_passed = passed;
	}

	public PersistitTestResult(final boolean passed, final String message) {
		_passed = passed;
		_message = message;
	}

	public PersistitTestResult(final boolean passed, final Throwable throwable) {
		_passed = passed;
		_throwable = throwable;
		_message = _throwable.toString();
	}

	public PersistitTestResult(final boolean passed, final PersistitTestResult[] results) {
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
				final PersistitTestResult result1 = _results[index];
				if ((result1 != null) && !result1._passed) {
					failCount++;
				}
			}

			TestRunner.logMessage("Multithreaded Test Failure: " + failCount
					+ " threads failed", depth);

			for (int index = 0; index < _results.length; index++) {
				final PersistitTestResult result1 = _results[index];
				if ((result1 != null) && !result1._passed) {
					result1.logFailure(depth + 1);
				}
			}
		} else if (_throwable != null) {
			TestRunner.logMessage(_throwable.toString(), depth);
			final String s = LogBase.detailString(_throwable).replace('\r',
					' ');
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

	public String toString() {
		if (_passed) {
			return "PASSED";
		} else {
			if (_results != null) {
				return "Multithreaded test FAILED";
			} else if (_throwable != null) {
				return "FAILED WITH EXCEPTION\r\n"
						+ LogBase.detailString(_throwable);
			} else if (_message != null) {
				return "FAILED: " + _message;
			}
			return "FAILED";
		}
	}
}