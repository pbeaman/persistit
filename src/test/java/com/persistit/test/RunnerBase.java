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

package com.persistit.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.persistit.Configuration;
import com.persistit.Configuration.BufferPoolConfiguration;
import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.VolumeSpecification;
import com.persistit.util.ArgParser;

public class RunnerBase {

    private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMddHHmm");

    private final static String[] ARGS_TEMPLATE = { "duration|int:1::60|Maximum duration in seconds",
            "path|String:/tmp/persistit_test_data|Data path",
            "progress|int:10::600|Progress message interval in seconds", };

    protected final static long PROGRESS_LOG_INTERVAL = 600000;

    protected final static long NS_PER_MS = 1000000;
    protected final static long MS_PER_S = 1000;
    private long nextReport;

    List<AbstractTestRunnerItem> _tests = new ArrayList<AbstractTestRunnerItem>();

    String _name;
    String _logPath;
    String _dataPath;
    long _duration;
    long _progressLogInterval;

    String _timeStamp = SDF.format(new Date());

    protected RunnerBase(final String name, final String[] args) {
        _name = name;
        final ArgParser ap = new ArgParser("RunnerBase", args, ARGS_TEMPLATE);
        _logPath = _dataPath = ap.getStringValue("path");
        _duration = ap.getLongValue("duration") * MS_PER_S;
        _progressLogInterval = ap.getLongValue("progress");
    }

    protected void add(AbstractTestRunnerItem test) {
        _tests.add(test);
    }

    protected void clear() {
        _tests.clear();
    }

    protected void execute(final Persistit persistit) {
        try {
            int index = 0;
            List<Thread> threads = new ArrayList<Thread>();
            for (AbstractTestRunnerItem test : _tests) {
                index++;
                test.initialize(index);
                test.setPersistit(persistit);
                threads.add(new Thread(test));
            }
            final long start = System.nanoTime();
            final long end = start + (NS_PER_MS * _duration);

            for (Thread thread : threads) {
                thread.start();
            }

            while (true) {
                final long now = System.nanoTime();
                if (poll(_tests, now - start, end - now) == 0) {
                    break;
                }
                if (now > end) {
                    for (AbstractTestRunnerItem test : _tests) {
                        test.forceStop();
                    }
                }
                Thread.sleep(MS_PER_S);
            }

            for (Thread thread : threads) {
                thread.join(MS_PER_S);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected int poll(final List<AbstractTestRunnerItem> tests, final long elapsed, final long remaining) {
        int live = 0;
        int failed = 0;
        int ended = 0;
        int stopped = 0;

        for (AbstractTestRunnerItem test : tests) {
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
        }

        if (elapsed > nextReport || remaining <= 0) {
            System.out.printf("%s at %,9d seconds: live=%,5d ended=%,5d stopped = %,5d, failed=%,5d\n", _name, elapsed
                    / NS_PER_MS / MS_PER_S, live, ended, stopped, failed);
            nextReport += (NS_PER_MS * MS_PER_S * _progressLogInterval);
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
                    if (child.getPath().startsWith(pattern.substring(0, pattern.length() - 1))) {
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
