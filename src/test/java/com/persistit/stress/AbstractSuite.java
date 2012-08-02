/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.stress;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.persistit.Configuration;
import com.persistit.Configuration.BufferPoolConfiguration;
import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.VolumeSpecification;
import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;

public abstract class AbstractSuite {

    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMddHHmm");

    private final static String[] ARGS_TEMPLATE = { "duration|int::10|Maximum duration in seconds",
            "datapath|String:/tmp/persistit_test_data|Data path",
            "progress|int:60:1:|Progress message interval in seconds", "_flag|S|Save on failure", };

    protected final static long PROGRESS_LOG_INTERVAL = 600000;

    protected final static long NS_PER_MS = 1000000;
    protected final static long MS_PER_S = 1000;
    protected final static long NS_PER_S = NS_PER_MS * MS_PER_S;

    private long _nextReport;
    private long _accumulatedWork;

    List<AbstractStressTest> _tests = new ArrayList<AbstractStressTest>();

    final private String _name;
    final private String _logPath;
    final private String _dataPath;
    final private long _progressLogInterval;
    final private boolean _saveOnFailure;

    private long _duration;
    private boolean _untilStopped;
    private long _elapsed;
    private boolean _failed;

    String _timeStamp = SDF.format(new Date());

    protected AbstractSuite(final String name, final String[] args) {
        _name = name;
        final ArgParser ap = new ArgParser(getClass().getSimpleName(), args, ARGS_TEMPLATE);
        String dataPath = ap.getStringValue("datapath");
        if (dataPath.endsWith("/") || dataPath.endsWith("\\")) {
            dataPath = dataPath.substring(0, dataPath.length() - 1);
         }
        _logPath = _dataPath = dataPath;
        _duration = ap.getLongValue("duration");
        _progressLogInterval = ap.getLongValue("progress");
        _untilStopped = ap.isSpecified("duration");
        _saveOnFailure = ap.isFlag('S');
    }

    public String getName() {
        return _name;
    }

    public long getDuration() {
        return _duration;
    }

    public void setDuration(final long duration) {
        _duration = duration;
    }

    public long getRate() {
        return _elapsed > 0 ? _accumulatedWork / _elapsed : 0;
    }

    public boolean isFailed() {
        return _failed;
    }

    public boolean isUntilStopped() {
        return _untilStopped;
    }

    public boolean takeUntilStopped() {
        if (_untilStopped) {
            _untilStopped = false;
            return true;
        } else {
            return false;
        }
    }

    protected void add(AbstractStressTest test) {
        _tests.add(test);
    }

    protected void clear() {
        _tests.clear();
        _nextReport = 0;
        _accumulatedWork = 0;
    }

    public abstract void runTest() throws Exception;

    protected void execute(final Persistit persistit) {
        try {
            int index = 0;
            List<Thread> threads = new ArrayList<Thread>();
            for (AbstractStressTest test : _tests) {
                index++;
                test.initialize(index);
                test.setPersistit(persistit);
                test.setUntilStopped(_untilStopped);
                threads.add(new Thread(test));
            }
            final long start = System.nanoTime();
            final long end = start + (NS_PER_S * _duration);

            for (Thread thread : threads) {
                thread.start();
            }

            while (true) {
                final long now = System.nanoTime();
                if (poll(_tests, now - start, end - now) == 0) {
                    break;
                }
                if (now > end && isUntilStopped()) {
                    for (AbstractStressTest test : _tests) {
                        test.forceStop();
                    }
                }
                Thread.sleep(MS_PER_S);
            }

            for (Thread thread : threads) {
                thread.join(MS_PER_S);
            }

            boolean failed = false;
            long work = 0;
            for (AbstractStressTest test : _tests) {
                if (test.isFailed()) {
                    failed = true;
                }
                work += test.getTotalWorkDone();
            }
            _elapsed = (System.nanoTime() - start) / NS_PER_S;
            System.out.printf("\n---Result %s: %s work=%,d time=%,d rate=%,d ---\n", this._name, failed ? "FAILED"
                    : "PASSED", work, _elapsed, _elapsed > 0 ? work / _elapsed : 0);

            if (failed && _saveOnFailure) {
                saveOnFailure();
            }
            _failed |= failed;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected int poll(final List<AbstractStressTest> tests, final long elapsed, final long remaining) {
        int live = 0;
        int failed = 0;
        int ended = 0;
        int stopped = 0;
        long work = 0;

        for (AbstractStressTest test : tests) {
            if (test.isFinished()) {
                ended++;
            } else {
                live++;
            }
            if (test.isFailed()) {
                failed++;
            } else if (test.isStopped()) {
                stopped++;
            }
            work += test.getTotalWorkDone();
        }

        if (_nextReport != 0 && (elapsed > _nextReport || remaining <= 0 && isUntilStopped())) {
            long rate = 0;
            if (elapsed > 0) {
                rate = (work * NS_PER_MS * MS_PER_S) / elapsed;
            }
            System.out.printf("%s at %,9d seconds: live=%,5d ended=%,5d stopped = %,5d, failed=%,5d "
                    + "totalwork=%,12d intervalwork=%,12d  workrate=%,12d\n", _name, elapsed / NS_PER_S, live, ended,
                    stopped, failed, work, work - _accumulatedWork, rate);
            _accumulatedWork = work;
        }

        if (elapsed > _nextReport) {
            _nextReport += (NS_PER_S * _progressLogInterval);
        }

        return live;
    }

    protected String substitute(final String s) {
        if (s.indexOf('$') == -1) {
            return s;
        }
        final StringBuilder sb = new StringBuilder(s);
        substitute(sb, "$logpath$", _logPath);
        substitute(sb, "$datapath$", _dataPath);
        substitute(sb, "$timestamp$", _timeStamp);
        return sb.toString();
    }

    private void substitute(final StringBuilder sb, final String from, final String to) {
        int index = -1;
        int offset = 0;
        final String s = sb.toString();
        while ((index = s.indexOf(from, index + 1)) != -1) {
            sb.replace(index + offset, index + offset + from.length(), to);
            offset += to.length() - from.length();
        }
    }

    protected void deleteFiles(final String pattern) {
        if (pattern.endsWith("*")) {
            File file = new File(pattern).getParentFile();
            if (file.isDirectory()) {
                final File[] files = file.listFiles();
                for (final File child : files) {
                    if (child.getPath().startsWith(pattern.substring(0, pattern.length() - 1))
                            && !child.getName().startsWith("_failed")) {
                        child.delete();
                        System.out.println("deleted " + child.toString());
                    }
                }
            }
        } else {
            final File file = new File(pattern);
            file.delete();
            System.out.println("deleted " + file.toString());
        }
    }

    protected void saveOnFailure() throws IOException {
        File dir = new File(_dataPath);
        File moveTo = new File(dir, String.format("_failed_%s_%2$tY%2$tm%2$td%2$tH%2$tM%2$tS", getName(), System
                .currentTimeMillis()));
        moveTo.mkdirs();

        final File[] files = dir.listFiles();
        for (final File child : files) {
            if (!child.isDirectory()) {
                File to = new File(moveTo, child.getName());
                boolean moved = child.renameTo(to);
                System.out.printf("%s %s to %s\n", moved ? "moved" : "failed to move", child, to);
            }
        }

        PrintWriter pw = new PrintWriter(new FileWriter(new File(moveTo, "results")));
        for (final AbstractStressTest test : _tests) {
            pw.printf("%s [%s] %s \n\n", test.getTestName(), test.getThreadName(), test.getResult());
        }
        pw.close();
    }

    protected Persistit makePersistit(final int pageSize, final String mem, final CommitPolicy policy)
            throws PersistitException {
        final Persistit persistit = new Persistit();
        persistit.initialize(makeConfiguration(pageSize, mem, policy));
        return persistit;

    }

    protected Configuration makeConfiguration(final int pageSize, final String mem, final CommitPolicy policy) {
        Configuration c = new Configuration();
        c.setCommitPolicy(policy);
        c.setJmxEnabled(true);
        c.setJournalPath(substitute("$datapath$/persistit_journal"));
        c.setLogFile(substitute("$datapath$/persistit_$timestamp$.log"));
        c.setRmiPort(8081);
        BufferPoolConfiguration bpc = c.getBufferPoolMap().get(pageSize);
        if (mem.contains(",")) {
            bpc.parseBufferMemory(pageSize, "buffer.memory." + pageSize, mem);
        } else {
            bpc.parseBufferCount(pageSize, "buffer.count." + pageSize, mem);
        }
        c.getVolumeList().add(
                new VolumeSpecification(substitute("$datapath$/persistit,create,pageSize:" + pageSize
                        + ",initialSize:100M,extensionSize:100M,maximumSize:500G")));
        return c;
    }
}
