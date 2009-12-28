package com.persistit.test;

import java.io.PrintStream;

import javax.swing.text.Document;

import junit.framework.TestCase;

import com.persistit.Persistit;
import com.persistit.unit.UnitTestProperties;

/**
 * Test classes must extend this class. The subclass must implement
 * executeTest(), and must post the result of the test to the _result field. The
 * subclass may optionally adjust the _status field and/or the
 * repetitionCount and _progressCount fields. The subclass must stop any
 * lengthy loop iteration in the event _forceStop is set to true.
 */
public abstract class PersistitScriptedTestCase extends TestCase {

	protected String[] _args = new String[0];
	
	protected Persistit _persistit;

	protected PersistitScriptedTestUnit _unit;
	/**
	 * Subclass posts Result to this
	 */
	protected PersistitTestResult _result = null;
	/**
	 * Subclass should
	 */
	protected String _name = getTestName() + ":" + getName();
	protected boolean _started = false;
	protected long _startTime;
	protected boolean _finished = false;
	protected long _finishTime;
	protected boolean _forceStop = false;
	protected String _status = "not started";

	protected int _threadIndex;

	PrintStream _out = System.out;
	PrintStream _err = System.err;
	Document _document;
	String _lastLoggedStatus;

	StringBuffer _sb = new StringBuffer();

	protected int _dotGranularity = 1000;

	@Override
	public void setUp() throws Exception {
		_persistit = new Persistit();
		_persistit.initialize(UnitTestProperties.getProperties());
	}

	@Override
	public void tearDown() throws Exception {
		_persistit.close();
		_persistit = null;
	}

	protected abstract void executeTest() throws Exception;

	protected abstract String shortDescription();

	protected abstract String longDescription();

	protected abstract String getProgressString();

	protected abstract double getProgress();

	protected void setPersistit(final Persistit persistit) {
		_persistit = persistit;
	}

	protected Persistit getPersistit() {
		return _persistit;
	}

	public String status() {
		if (isPassed()) {
			return "PASSED (" + (_finishTime - _startTime) + "ms)";
		} else if (isFailed()) {
			return "FAILED";
		} else if (isStopped()) {
			return "STOPPED";
		} else if (isStarted()) {
			return "RUNNING";
		} else {
			return "NOT STARTED";
		}
	}

	void initialize(final int index) {
		_result = null;
		_started = false;
		_finished = false;
		_forceStop = false;
		_threadIndex = index;
		_status = "not started";
	}

	public void runIt() {
		try {
			Thread.sleep(1000);
		} catch (final InterruptedException ie) {
		}
		_started = true;
		_startTime = ScriptedTestRunner.ts();

		println("Starting unit=" + getUnitName() + " test=" + getTestName()
				+ " " + getName() + " at ts=" + _startTime);
		println();

		try {
			executeTest();
		} catch (final Throwable t) {
			if ((t instanceof RuntimeException)
					&& "STOPPED".equals(t.getMessage())) {
				println();
				println("STOPPED");
				if (_result == null) {
					_result = new PersistitTestResult(false, t);
				}
			} else {
				println();
				println("Failed: " + t);
				t.printStackTrace(_out);
				println(t.getMessage());
				if ((_result == null) || _result._passed) {
					_result = new PersistitTestResult(false, t);
				}
			}
		}
		_finishTime = ScriptedTestRunner.ts();
		println("Finished unit=" + getUnitName() + " test=" + getTestName()
				+ " " + getName() + " at ts=" + _finishTime + " - elapsed="
				+ (_finishTime - _startTime) + " - "
				+ (_result == null ? "PASSED" : _result.toString()));
		println();
		if ((_result != null) && !_result._passed) {
			ScriptedTestRunner.logMessage("Failed test unit=" + getUnitName() + " test="
					+ getTestName() + " " + getName());
			_result.logFailure();
		}
		_finished = true;
	}

	protected synchronized PersistitTestResult getResult() {
		return _result;
	}

	protected synchronized boolean isStarted() {
		return _started;
	}

	protected synchronized boolean isFinished() {
		return _finished;
	}

	protected synchronized String getStatus() {
		return _status;
	}

	protected synchronized void forceStop() {
		_forceStop = true;
	}

	protected synchronized boolean isStopped() {
		return _forceStop;
	}

	protected synchronized boolean isPassed() {
		if (isFinished()) {
			return (_result == null) || _result._passed;
		}
		return false;
	}

	protected synchronized boolean isFailed() {
		if (isFinished()) {
			return (_result != null) && !_result._passed;
		}
		return false;
	}

	protected void runStandalone(final String[] args) throws Exception {
		if (_unit == null) {
			_unit = new PersistitScriptedTestUnit("Standalone", 0);
		}
		ScriptedTestRunner._verbose = true;
		println();
		println("-> " + toString() + " - " + shortDescription());
		println();
		println(longDescription());
		println();
		_args = args;
		setUp();
		executeTest();
		tearDown();
		_persistit.close();

		println();
		println("<- " + toString() + " - " + status());
		if (isFailed()) {
			_result.logFailure();
		}
		println();
	}

	public String toString() {
		return _name;
	}

	protected String getTestName() {
		final String s = getClass().getName();
		final int p = s.lastIndexOf('.');
		return s.substring(p + 1);
	}

	protected String getUnitName() {
		return _unit._name;
	}

	protected PrintStream getOutputStream() {
		return _out;
	}

	protected void setOutputStream(final PrintStream ps) {
		_out = ps;
	}

	protected void setErrorStream(final PrintStream ps) {
		_err = ps;
	}

	protected PrintStream getErrorStream() {
		return _err;
	}

	protected void setDocument(final Document document) {
		_document = document;
	}

	protected Document getDocument() {
		return _document;
	}

	protected String tsString() {
		long time = ScriptedTestRunner.ts();
		_sb.setLength(0);
		_sb.append("         ");
		for (int i = 8; --i >= 0;) {
			final char ch = (char) ((time % 10) + '0');
			_sb.setCharAt(i, ch);
			time /= 10;
			if (time == 0) {
				break;
			}
		}
		return _sb.toString();
	}

	protected void println() {
		_out.println();
		_out.flush();
	}

	protected void println(final Object o) {
		_out.println(o);
		_out.flush();
	}

	protected void print(final Object o) {
		_out.print(o);
		_out.flush();
	}

	protected void printStackTrace(final Throwable t) {
		t.printStackTrace(_err);
	}

	public String getName() {
		return Thread.currentThread().getName();
	}
}