/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.stress;

import static com.persistit.util.Util.MS_PER_S;
import static com.persistit.util.Util.NS_PER_MS;
import static com.persistit.util.Util.NS_PER_S;

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
            "progress|int:60:1:|Progress message interval in seconds", "_flag|S|Save on failure",
            "checkpointinterval|int:120:10:3600|Checkpoint interval in seconds",
            "commitpolicy|String:|Default commit policy", "memoryadjustment|String:|Memory adjustment" };

    protected final static long PROGRESS_LOG_INTERVAL = 600000;

    private long _nextReport;
    private long _accumulatedWork;

    List<AbstractStressTest> _tests = new ArrayList<AbstractStressTest>();

    final private String _name;
    final private String _logPath;
    final private String _dataPath;
    final private long _progressLogInterval;
    final private boolean _saveOnFailure;
    private final int _checkpointInterval;
    private final String _commitPolicyOverride;
    private final String _memoryAdjustment;

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
        _checkpointInterval = ap.isSpecified("checkpointinterval") ? ap.getIntValue("checkpointinterval") : 0;
        _commitPolicyOverride = ap.getStringValue("commitpolicy");
        _memoryAdjustment = ap.getStringValue("memoryadjustment");
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

    protected void add(final AbstractStressTest test) {
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
            final List<Thread> threads = new ArrayList<Thread>();
            for (final AbstractStressTest test : _tests) {
                index++;
                test.initialize(index);
                test.setPersistit(persistit);
                test.setUntilStopped(_untilStopped);
                threads.add(new Thread(test));
            }
            final long start = System.nanoTime();
            final long end = start + (NS_PER_S * _duration);

            for (final Thread thread : threads) {
                thread.start();
            }

            while (true) {
                final long now = System.nanoTime();
                if (poll(_tests, now - start, end - now) == 0) {
                    break;
                }
                if (now > end && isUntilStopped()) {
                    for (final AbstractStressTest test : _tests) {
                        test.forceStop();
                    }
                }
                Thread.sleep(MS_PER_S);
            }

            for (final Thread thread : threads) {
                thread.join(MS_PER_S);
            }

            boolean failed = false;
            long work = 0;
            for (final AbstractStressTest test : _tests) {
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
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected int poll(final List<AbstractStressTest> tests, final long elapsed, final long remaining) {
        int live = 0;
        int failed = 0;
        int ended = 0;
        int stopped = 0;
        long work = 0;

        for (final AbstractStressTest test : tests) {
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
            final File file = new File(pattern).getParentFile();
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
        final File dir = new File(_dataPath);
        final File moveTo = new File(dir, String.format("_failed_%s_%2$tY%2$tm%2$td%2$tH%2$tM%2$tS", getName(),
                System.currentTimeMillis()));
        moveTo.mkdirs();

        final File[] files = dir.listFiles();
        for (final File child : files) {
            if (!child.isDirectory()) {
                final File to = new File(moveTo, child.getName());
                final boolean moved = child.renameTo(to);
                System.out.printf("%s %s to %s\n", moved ? "moved" : "failed to move", child, to);
            }
        }

        final PrintWriter pw = new PrintWriter(new FileWriter(new File(moveTo, "results")));
        for (final AbstractStressTest test : _tests) {
            pw.printf("%s [%s] %s \n\n", test.getTestName(), test.getThreadName(), test.getResult());
        }
        pw.close();
    }

    protected Persistit makePersistit(final int pageSize, final String mem, final CommitPolicy policy)
            throws PersistitException {
        return new Persistit(makeConfiguration(pageSize, mem, policy));
    }

    protected Configuration makeConfiguration(final int pageSize, final String mem, final CommitPolicy policy) {
        final Configuration c = new Configuration();
        c.setCommitPolicy(overrideCommitPolicy(policy, _commitPolicyOverride));
        c.setJmxEnabled(true);
        c.setJournalPath(substitute("$datapath$/persistit_journal"));
        c.setLogFile(substitute("$datapath$/persistit_$timestamp$.log"));
        c.setRmiPort(8081);
        final BufferPoolConfiguration bpc = c.getBufferPoolMap().get(pageSize);
        if (mem.contains(",")) {
            bpc.parseBufferMemory(pageSize, "buffer.memory." + pageSize, mem);
        } else {
            bpc.parseBufferCount(pageSize, "buffer.count." + pageSize, mem);
        }
        c.getVolumeList().add(
                new VolumeSpecification(substitute("$datapath$/persistit,create,pageSize:" + pageSize
                        + ",initialSize:100M,extensionSize:100M,maximumSize:500G")));
        if (_checkpointInterval > 0) {
            c.setCheckpointInterval(_checkpointInterval);
        }
        adjustMemory(bpc);
        return c;
    }

    private CommitPolicy overrideCommitPolicy(final CommitPolicy policy, final String override) {
        return override != null && !override.isEmpty() ? CommitPolicy.valueOf(override) : policy;
    }

    private void adjustMemory(final BufferPoolConfiguration bpc) {
        final int minc = bpc.getMinimumCount();
        final int maxc = bpc.getMaximumCount();
        final long mins = bpc.getMinimumMemory();
        final long maxs = bpc.getMaximumMemory();
        if (_memoryAdjustment == null || _memoryAdjustment.isEmpty()) {
            return;
        }
        if (_memoryAdjustment.startsWith("x")) {
            final float fraction = Float.parseFloat(_memoryAdjustment.substring(1));
            if (minc > 0 && maxc < Integer.MAX_VALUE) {
                bpc.setMinimumCount((int) (minc * fraction));
                bpc.setMaximumCount((int) (maxc * fraction));
            } else {
                if (mins > 0 && maxs < Long.MAX_VALUE) {
                    bpc.setMinimumMemory((long) (mins * fraction));
                    bpc.setMaximumMemory((long) (maxs * fraction));
                }
            }
        } else if (_memoryAdjustment.startsWith("c")) {
            final int count = Integer.parseInt(_memoryAdjustment.substring(1));
            bpc.setMinimumCount(count);
            bpc.setMaximumCount(count);
        } else {
            final long size = Configuration.parseLongProperty("MemoryAdjustment", _memoryAdjustment);
            bpc.setMinimumCount(0);
            bpc.setMaximumCount(Integer.MAX_VALUE);
            bpc.setMinimumMemory(0);
            bpc.setMaximumMemory(size);
        }
    }
}
