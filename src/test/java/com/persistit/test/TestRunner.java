/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Apr 12, 2004
 */
package com.persistit.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.swing.text.Document;

import junit.framework.TestCase;

import com.persistit.ArgParser;
import com.persistit.LogBase;
import com.persistit.Persistit;
import com.persistit.Util;

/**
 * 
 * Framework for running tests on Persistit.
 * 
 * Runs various tests, records progress and results in a log file. This is
 * intended to permit long-term unattended testing.
 * 
 */
public class TestRunner {

    private final static int PROGRESS_LOG_INTERVAL = 600000;

    private final static int CODE_TEST = 1;
    private final static int CODE_LOG = 2;
    private final static int CODE_LOGPATH = 3;
    private final static int CODE_DELETE = 4;
    private final static int CODE_RUN = 5;
    private final static int CODE_INIT = 6;
    private final static int CODE_CLOSE = 7;
    private final static int CODE_MULTI = 8;
    private final static int CODE_WAIT = 9;
    private final static int CODE_WAIT_IF = 10;
    private final static int CODE_UNIT_TEST = 11;

    private final static String SPACES = "                                   ";

    private final static String[] ARGS_TEMPLATE =
        { "script|String:./testscript.txt|Script file name",
            "logpath|String:./", "datapath|String:./data", "select|String:*",
            "_flag|v|Verbose log to console",
            "_flag|g|Launch diagnostic GUI on Persistit init", };

    private final static String[] EMPTY_STRING_ARRAY = new String[0];
    private final static Result[] EMPTY_RESULT_ARRAY = new Result[0];

    // defaults
    private static String _scriptPath;
    private static String _logPath;
    private static String _dataPath;

    private static String _timeStamp;

    private static SimpleDateFormat _sdf = new SimpleDateFormat("yyyyMMddHHmm");

    private static SimpleDateFormat _sdfExternal =
        new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    private static PrintWriter _logWriter = null;
    private static PrintWriter _displayWriter = new PrintWriter(System.out);
    private static long _testLogTime = System.currentTimeMillis();
    private static boolean _verbose;
    private static boolean _verboseGui;
    private static boolean _failureArmed = false;

    private static boolean _stopAll = false;

    protected Persistit _persistit = new Persistit();


    private boolean _scriptError = false;

    private final ArrayList<Unit> _units = new ArrayList<Unit>();
    private final ArrayList<Test> _allTests = new ArrayList<Test>();

    private TestRunnerGui _gui;

    /**
     * Runs a series of tests.
     * 
     * @param args
     */
    public static void main(final String[] args) throws Exception {
        final TestRunner runner = new TestRunner();
        runner.parseScript(args);
        if (!runner._scriptError) {
            runner.execute();
        }
    }

    public void execute() {
        if (_scriptError) {
            throw new RuntimeException("Script errors - cannot execute");
        }

        final int count = _units.size();
        int passCount = 0;
        int failCount = 0;
        _timeStamp = _sdf.format(new Date());

        new ProgressLogger().start();
        for (int index = 0; !_stopAll && (index < count); index++) {
            final Unit unit = (Unit) _units.get(index);
            logMessage();
            logMessage("Unit " + unit._name + " started");
            final Result result = unit.execute();
            if (result._passed) {
                passCount++;
                logMessage("Unit " + unit._name + " PASSED");
            } else {
                logMessage("Unit " + unit._name + " FAILED");
                for (int i = 0; i < result._results.length; i++) {
                    final Result r = result._results[i];
                    if ((r != null) && !r._passed) {
                        r.logFailure();
                    }
                }
                failCount++;
            }
        }
        logMessage();
        logMessage("PASSED=" + passCount + " FAILED=" + failCount);
    }

    public void parseScript(final String[] args) throws Exception {
        final ArgParser ap =
            new ArgParser("com.persistit.TestRunner", args, ARGS_TEMPLATE);

        _verbose = ap.isFlag('v');
        _verboseGui = ap.isFlag('g');
        _dataPath = ap.getStringValue("datapath");
        _logPath = ap.getStringValue("logpath");
        _scriptPath = ap.getStringValue("script");

        final BufferedReader script =
            new BufferedReader(new FileReader(ap.getStringValue("script")));

        Unit unit = new Unit("#0 initialize", 0);
        _units.add(unit);

        ArrayList<Command> concurrentCommands = null;

        Properties properties = new Properties();
        boolean propertiesAssigned = false;
        boolean skip = false;

        for (int lineNumber = 1;; lineNumber++) {
            String line = script.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();

            // comment
            if (line.startsWith("//")) {
                continue;
            }

            if (line.startsWith("/*")) {
                skip = true;
                continue;
            }

            if (line.startsWith("*/")) {
                skip = false;
                continue;
            }

            if (skip) {
                continue;
            }

            // test unit identifier
            if (line.startsWith("#")) {
                if (concurrentCommands != null) {
                    unit._commands.add(new Command(CODE_MULTI,
                        concurrentCommands));
                    concurrentCommands = null;
                }

                unit = new Unit(line, lineNumber);
                _units.add(unit);
                continue;
            }

            // commands
            final StringTokenizer st = new StringTokenizer(line, " ");

            // blank line
            if (!st.hasMoreTokens()) {
                continue;
            }

            final String keyw = st.nextToken();

            if ("test".equalsIgnoreCase(keyw)) {
                Class clazz = null;
                Test test = null;
                if (!st.hasMoreElements()) {
                    error("Missing Test class name", lineNumber, line);
                }
                try {
                    clazz = Class.forName(st.nextToken());
                    test = (Test) (clazz.newInstance());
                    test.setPersistit(_persistit);
                } catch (final Exception e) {
                    error(e.toString(), lineNumber, line);
                }

                if (!st.hasMoreTokens()) {
                    error("Argument is required", lineNumber, line);
                }
                test._unit = unit;
                final Command command = new Command(CODE_TEST, args(st), test);
                if (concurrentCommands != null) {
                    concurrentCommands.add(command);
                } else {
                    unit._commands.add(command);
                }
                continue;
            }

            if (concurrentCommands != null) {
                unit._commands.add(new Command(CODE_MULTI, concurrentCommands));
                concurrentCommands = null;
            }

            if ("clearprops".equalsIgnoreCase(keyw)) {
                properties = new Properties();
                propertiesAssigned = false;
                continue;
            }

            if ("prop".equalsIgnoreCase(keyw)) {
                if (propertiesAssigned) {
                    properties = (Properties) properties.clone();
                    propertiesAssigned = false;
                }
                final String rest = line.substring("prop".length());
                final int p = rest.indexOf("=");
                if ((p <= 0) || (p >= rest.length())) {
                    error("Bad property specification", lineNumber, line);
                    continue;
                }
                final String attr = rest.substring(0, p).trim();
                final String value = rest.substring(p + 1).trim();
                properties.put(substitute(attr), substitute(value));
                continue;
            }

            if ("log".equalsIgnoreCase(keyw)) {
                final Command command = new Command(CODE_LOG, args(st));
                unit._commands.add(command);
            }

            if ("logpath".equalsIgnoreCase(keyw)) {
                final Command command = new Command(CODE_LOGPATH, args(st));
                unit._commands.add(command);
                continue;
            }

            if ("delete".equalsIgnoreCase(keyw)) {
                final Command command = new Command(CODE_DELETE, args(st));
                unit._commands.add(command);
                continue;
            }

            if ("run".equalsIgnoreCase(keyw)) {
                if (!st.hasMoreTokens()) {
                    error("Argument is required", lineNumber, line);
                }

                final Command command = new Command(CODE_RUN, args(st));
                unit._commands.add(command);
                continue;
            }

            if ("init".equalsIgnoreCase(keyw)) {
                if (st.hasMoreTokens()) {
                    error("No arguments permitted", lineNumber, line);
                }
                final Command command = new Command(CODE_INIT, properties);
                unit._commands.add(command);
                continue;
            }

            if ("close".equalsIgnoreCase(keyw)) {
                if (st.hasMoreTokens()) {
                    error("No arguments permitted", lineNumber, line);
                }
                final Command command = new Command(CODE_CLOSE);
                unit._commands.add(command);
                continue;
            }

            if ("icheck".equalsIgnoreCase(keyw)) {
                if (!st.hasMoreTokens()) {
                    error("Argument is required", lineNumber, line);
                }
                final Test test = new IntegrityTest();
                test.setPersistit(_persistit);
                test._unit = unit;
                final Command command = new Command(CODE_TEST, args(st), test);
                if (concurrentCommands != null) {
                    concurrentCommands.add(command);
                } else {
                    unit._commands.add(command);
                }
                continue;
            }

            if ("multithread".equalsIgnoreCase(keyw)
                && (concurrentCommands == null)) {
                concurrentCommands = new ArrayList();
                continue;
            }

            if ("wait".equalsIgnoreCase(keyw)) {
                final Command command = new Command(CODE_WAIT, args(st));
                unit._commands.add(command);
                continue;
            }

            if ("waitIfFailed".equalsIgnoreCase(keyw)) {
                final Command command = new Command(CODE_WAIT_IF, args(st));
                unit._commands.add(command);
                continue;
            }

            error("Invalid keyword", lineNumber, line);
        }
    }

    private String[] args(final StringTokenizer st) {
        final ArrayList list = new ArrayList();
        while (st.hasMoreElements()) {
            final String item = st.nextToken();
            list.add(item);
        }
        return (String[]) list.toArray(EMPTY_STRING_ARRAY);
    }

    private void error(final String msg, final int lineNumber, final String line) {
        System.out.println(msg + " on line " + lineNumber + ":");
        System.out.println("  " + line);
        System.out.println();
        _scriptError = true;
    }

    private static class Unit {
        String _name;
        int _lineNumber;
        ArrayList _commands = new ArrayList();
        boolean _passed = true; // Until failure

        Unit(final String name, final int lineNumber) {
            _name = name;
            _lineNumber = lineNumber;
        }

        public Result execute() {
            final ArrayList results = new ArrayList();
            final int size = _commands.size();
            for (int index = 0; !_stopAll && (index < size); index++) {
                final Result result =
                    ((Command) _commands.get(index)).execute();
                if (result != null) {
                    results.add(result);
                    if (!result._passed) {
                        _passed = false;
                    }
                }
            }
            return new Result(_passed, (Result[]) results
                .toArray(EMPTY_RESULT_ARRAY));
        }
    }

    public static class Result {
        boolean _passed;
        String _message;
        Throwable _throwable;
        Result[] _results;

        long _timestamp = ts();
        long _elapsedTime = 0;

        protected Result(final boolean passed) {
            _passed = passed;
        }

        public Result(final boolean passed, final String message) {
            _passed = passed;
            _message = message;
        }

        public Result(final boolean passed, final Throwable throwable) {
            _passed = passed;
            _throwable = throwable;
            _message = _throwable.toString();
        }

        public Result(final boolean passed, final Result[] results) {
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
                    final Result result1 = _results[index];
                    if ((result1 != null) && !result1._passed) {
                        failCount++;
                    }
                }

                logMessage("Multithreaded Test Failure: " + failCount
                    + " threads failed", depth);

                for (int index = 0; index < _results.length; index++) {
                    final Result result1 = _results[index];
                    if ((result1 != null) && !result1._passed) {
                        result1.logFailure(depth + 1);
                    }
                }
            } else if (_throwable != null) {
                logMessage(_throwable.toString(), depth);
                final String s =
                    LogBase.detailString(_throwable).replace('\r', ' ');
                final StringTokenizer st = new StringTokenizer(s, "\n");
                while (st.hasMoreTokens()) {
                    logMessage(st.nextToken(), depth + 1);
                    if (!_verbose) {
                        break;
                    }
                }
            } else if (_message != null) {
                logMessage(_message, depth);
            } else {
                logMessage("Unspecified failure", depth);
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

    public static class TestThread extends Thread {
        private final Test _test;

        private TestThread(final Test test) {
            this._test = test;
        }

        public void run() {
            _test.runIt();
        }
    }

    /**
     * Test classes must extend this class. The subclass must implement
     * runTest(), and must post the result of the test to the _result field. The
     * subclass may optionally adjust the _status field and/or the
     * repetitionCount and _progressCount fields. The subclass must stop any
     * lengthy loop iteration in the event _forceStop is set to true.
     */
    public static abstract class Test extends TestCase {

        protected Persistit _persistit;
        
        protected Unit _unit;
        /**
         * Subclass posts Result to this
         */
        protected Result _result = null;
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
        private String _lastLoggedStatus;

        StringBuffer _sb = new StringBuffer();
        
        protected int _dotGranularity = 1000;

        protected abstract void runTest();

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

        private void initialize(final int index) {
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
            _startTime = ts();

            println("Starting unit=" + getUnitName() + " test=" + getTestName()
                + " " + getName() + " at ts=" + _startTime);
            println();

            try {
                runTest();
            } catch (final Throwable t) {
                if ((t instanceof RuntimeException)
                    && "STOPPED".equals(t.getMessage())) {
                    println();
                    println("STOPPED");
                    if (_result == null) {
                        _result = new Result(false, t);
                    }
                } else {
                    println();
                    println("Failed: " + t);
                    t.printStackTrace(_out);
                    println(t.getMessage());
                    if ((_result == null) || _result._passed) {
                        _result = new Result(false, t);
                    }
                }
            }
            _finishTime = ts();
            println("Finished unit=" + getUnitName() + " test=" + getTestName()
                + " " + getName() + " at ts=" + _finishTime + " - elapsed="
                + (_finishTime - _startTime) + " - "
                + (_result == null ? "PASSED" : _result.toString()));
            println();
            if ((_result != null) && !_result._passed) {
                logMessage("Failed test unit=" + getUnitName() + " test="
                    + getTestName() + " " + getName());
                _result.logFailure();
            }
            _finished = true;
        }

        protected void setupTest(final String[] args) {
        }

        protected void tearDownTest() {
        }

        protected synchronized Result getResult() {
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
                _unit = new Unit("Standalone", 0);
            }
            _verbose = true;
            println();
            println("-> " + toString() + " - " + shortDescription());
            println();
            println(longDescription());
            println();

            _persistit.initialize();
            setupTest(args);
            run();
            tearDownTest();
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
            long time = ts();
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

    private class Command {
        private final int _code;
        private String[] _args;
        Properties _properties;
        private Test _test;
        ArrayList<Command> _concurrentTests;

        Command(final int code) {
            _code = code;
        }

        Command(final int code, final String[] args) {
            _code = code;
            _args = args;
        }

        Command(final int code, final String[] args, final Test test) {
            _code = code;
            _args = args;
            _test = test;
        }

        Command(final int code, final Properties properties) {
            _code = code;
            _properties = properties;
        }

        Command(final int code, final ArrayList<Command> concurrentTests) {
            _code = code;
            _concurrentTests = concurrentTests;
        }

        private Result execute() {
            if (_stopAll) {
                return null;
            }
            final int argCount = _args == null ? 0 : _args.length;
            final String[] args = new String[argCount];
            for (int index = 0; index < argCount; index++) {
                args[index] = substitute(_args[index]);
            }

            try {
                switch (_code) {
                // This is the main case that runs test threads.
                case CODE_TEST: {
                    final Test test = _test;
                    test.initialize(0);
                    test.setupTest(args);
                    if (_gui != null) {
                        _gui.addTest(test);
                    }
                    _allTests.add(_test);
                    test.runIt();
                    test.tearDownTest();
                    return test.getResult();
                }

                case CODE_MULTI: {
                    final int threadCount =
                        _concurrentTests == null ? 0 : _concurrentTests.size();

                    final Thread[] threads = new Thread[threadCount];

                    for (int index = 0; index < threadCount; index++) {
                        final Command command =
                            (Command) _concurrentTests.get(index);
                        final Test test = command._test;
                        test.initialize(index);
                        test.setupTest(command._args);
                        if (_gui != null) {
                            _gui.addTest(test);
                        }
                    }
                    for (int index = 0; index < threadCount; index++) {
                        final Command command =
                            (Command) _concurrentTests.get(index);
                        final Test test = command._test;
                        _allTests.add(test);
                        threads[index] = new TestThread(test);
                        threads[index].start();
                    }
                    for (int index = 0; index < threadCount; index++) {
                        threads[index].join();
                    }
                    for (int index = 0; index < threadCount; index++) {
                        final Command command =
                            (Command) _concurrentTests.get(index);
                        command._test.tearDownTest();
                    }
                    final Result[] results = new Result[threadCount];
                    boolean pass = true;
                    for (int index = 0; index < threadCount; index++) {
                        final Command command =
                            (Command) _concurrentTests.get(index);
                        final Result result = command._test.getResult();
                        results[index] = result;
                        if ((result != null) && !result._passed) {
                            pass = false;
                        }
                    }
                    final Result result = new Result(pass, results);
                    return result;
                }

                case CODE_INIT: {
                    _persistit.close();
                    try {
                        _persistit.initialize(_properties);
                        if (_verboseGui) {
                            _persistit.setupGUI(false);
                        }
                        logMessage("initialized Persistit instance: "
                            + memcheck());
                        return null;
                    } catch (final Throwable t) {
                        _stopAll = true;
                        logMessage("initialization threw " + t);
                        return new Result(false, t);
                    }
                }

                case CODE_CLOSE: {
                    // This forcefully shuts down the diagnostic GUI
                    if (_persistit.isInitialized()) {
                        _persistit.shutdownGUI();
                    }
                    _persistit.close();
                    logMessage("closed Persistit instance: " + memcheck());
                    return null;
                }

                case CODE_LOG: {
                    final StringBuffer sb = new StringBuffer();
                    for (int i = 0; i < args.length; i++) {
                        if (i > 0) {
                            sb.append(' ');
                        }
                        sb.append(args[i]);
                    }
                    logMessage(sb.toString());
                    return null;
                }

                case CODE_LOGPATH: {
                    final String logFileName = args[0];
                    final PrintWriter logWriter =
                        new PrintWriter(new FileWriter(logFileName));
                    if (_logWriter != null) {
                        _logWriter.close();
                    }
                    _logWriter = logWriter;
                    startLog();
                    return null;
                }

                case CODE_DELETE: {
                    for (int index = 0; index < args.length; index++) {
                        final String fileName = args[index];
                        final File file = new File(fileName);
                        if (file.exists()) {
                            file.delete();
                            logMessage("deleted " + file.toString());
                        }
                    }
                    return null;
                }
                case CODE_WAIT:
                case CODE_WAIT_IF: {
                    if (_code == CODE_WAIT_IF) {
                        if (!_failureArmed) {
                            return null;
                        } else {
                            _failureArmed = false;
                        }
                    }
                    long expiration = Long.MAX_VALUE;
                    int index = 0;
                    try {
                        if (args.length > 0) {
                            expiration =
                                Long.parseLong(args[0])
                                    + System.currentTimeMillis();
                            index++;
                        }
                    } catch (final NumberFormatException nfe) {
                    }
                    final StringBuffer sb = new StringBuffer();
                    for (; index < args.length; index++) {
                        if (sb.length() > 0) {
                            sb.append(' ');
                        }
                        sb.append(args[index]);
                    }
                    final String message = sb.toString();
                    logMessage("waiting for user signal: " + message);
                    System.out.println();
                    System.out.println(message);
                    System.out.print("Press ENTER to proceed");
                    final boolean done = false;
                    while (System.currentTimeMillis() < expiration) {
                        try {
                            Thread.sleep(250);
                        } catch (final InterruptedException ie) {
                        }
                        if (System.in.available() > 0) {
                            final int c = System.in.read();
                            if ((c == 10) || (c == 13)) {
                                break;
                            }
                        }
                    }
                    System.out.println();
                    logMessage("continuing");
                }
                default: {
                    return null;
                }
                }
            } catch (final Exception e) {
                return new Result(false, e);
            }
        }
    }

    private class ProgressLogger extends Thread {
        ProgressLogger() {
            setName("Progress_Logger");
            setDaemon(true);
        }

        public void run() {
            final StringBuffer sb = new StringBuffer();
            boolean changed = true;
            for (;;) {
                try {
                    Thread.sleep(PROGRESS_LOG_INTERVAL);
                } catch (final InterruptedException ie) {
                }
                sb.setLength(0);
                for (int index = 0; index < _allTests.size(); index++) {
                    final Test test = _allTests.get(index);
                    if (test != null) {
                        String status;
                        if (test.isStarted() && !test.isFinished()) {
                            status = test.getProgressString();
                        } else {
                            status = test.status();
                        }
                        if (!status.equals(test._lastLoggedStatus)) {
                            test._lastLoggedStatus = status;
                            if (sb.length() == 0) {
                                sb.append("Progress report - changed status:");
                            }
                            sb.append("\r\n        ");
                            sb.append(test.getUnitName());
                            sb.append("  ");
                            sb.append(test.getTestName());
                            sb.append("  ");
                            sb.append(test.getName());
                            sb.append("  ");
                            sb.append(status);
                        }
                    }
                }
                if (sb.length() == 0) {
                    // suppress repeated no change log messages
                    if (changed) {
                        logMessage("Progress report: no changes");
                    }
                    changed = false;
                } else {
                    changed = true;
                    logMessage(sb.toString());
                }

            }
        }
    }

    /**
     * Make stock substitutions
     * 
     * @param s
     * @return
     */
    private String substitute(final String s) {
        if (s.indexOf('$') == -1) {
            return s;
        }
        final StringBuffer sb = new StringBuffer(s);
        substitute(sb, "$logpath$", _logPath);
        substitute(sb, "$datapath$", _dataPath);
        substitute(sb, "$timestamp$", _timeStamp);
        return sb.toString();
    }

    private void substitute(final StringBuffer sb, final String from,
        final String to) {
        int index = -1;
        int offset = 0;
        final String s = sb.toString();
        while ((index = s.indexOf(from, index + 1)) != -1) {
            sb.replace(index + offset, index + offset + from.length(), to);
            offset += to.length() - from.length();
        }
    }

    private void startLog() throws Exception {
        _logWriter.println("Persistit Test Run "
            + _sdfExternal.format(new Date()));
        _logWriter.println("Persistit version " + Persistit.version());
        _logWriter.println("JVM " + System.getProperty("java.vm.name") + " "
            + System.getProperty("java.vm.vendor") + " "
            + System.getProperty("java.vm.version"));
        _logWriter.println(" OS " + System.getProperty("os.name") + " "
            + System.getProperty("os.version") + " "
            + System.getProperty("os.arch"));
        _logWriter.println();
        _logWriter.println("Test Script");
        _logWriter.println("=====================");
        final BufferedReader script =
            new BufferedReader(new FileReader(_scriptPath));
        for (;;) {
            final String line = script.readLine();
            if (line == null) {
                break;
            }
            _logWriter.println(line);
        }
        script.close();
        _logWriter.println("=====================");
        _logWriter.println();
    }

    private static void logMessage() {
        logMessage(null, 0);
    }

    private static void logMessage(final String message) {
        logMessage(message, 0);
    }

    private static void logMessage(final String message, final int depth) {
        if (_logWriter != null) {
            logMessage(message, _logWriter, depth);
        }

        if (_displayWriter != null) {
            logMessage(message, _displayWriter, depth);
        }
    }

    private static void logMessage(final String message, final PrintWriter pw,
        final int depth) {
        if (message == null) {
            pw.println();
        } else {
            if (depth == 0) {
                pw.print(Util.format(ts(), 8));
            } else {
                pw.print(SPACES.substring(0, 8 + (depth * 2)));
            }
            pw.print(" ");
            pw.println(message);
        }
        pw.flush();
    }

    public static long ts() {
        return System.currentTimeMillis() - _testLogTime;
    }

    public void setGui(final TestRunnerGui gui) {
        _gui = gui;
    }

    public TestRunnerGui getGui() {
        return _gui;
    }

    static String memcheck() {
        long inUseStable = -1;
        for (int retries = 5; --retries >= 0;) {
            System.gc();
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ie) {
            }
            final long free = Runtime.getRuntime().freeMemory();
            final long total = Runtime.getRuntime().totalMemory();
            final long inUse = total - free;
            if ((retries <= 2) && (Math.abs(inUseStable - inUse) < 128)) {
                return " free=" + free + " total=" + total + " inUse=" + inUse;
            } else {
                inUseStable = inUse;
            }
        }
        return "Not stable after 10 seconds: lastEstimated inUse="
            + inUseStable;
    }

}
