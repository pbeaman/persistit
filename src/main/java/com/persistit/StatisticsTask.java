/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.Management.BufferPoolInfo;
import com.persistit.Management.JournalInfo;
import com.persistit.Management.TransactionInfo;
import com.persistit.mxbeans.IOMeterMXBean;
import com.persistit.util.Util;

/**
 * Task that reports, either once or periodically, various runtime statistics.
 * 
 * @author peter
 */
public class StatisticsTask extends Task {

    static final long NANOS_PER_MILLI = 1000000;
    static final long NANOS_PER_SECOND = 1000000000;

    final static String COUNT_FORMAT = "%s=%d";
    final static String RATE_FORMAT = "%s=%.3f";

    final static String PHEADER_FORMAT = " %10s";
    final static String PCOUNT_FORMAT = " %,10d";
    final static String PRATE_FORMAT = " %,10.3f";

    final static String TIME_HEADER_FORMAT = "%12s ";
    final static String TIME_FORMAT = "%,12d ";

    long _delay;
    long _count;
    boolean _bpool;
    boolean _journal;
    boolean _io;
    boolean _transactions;
    boolean _showRate;
    boolean _showValue;
    String _fileName;
    PrintWriter _pw;
    String _lastUpdate = "none";

    private final Map<String, Stat> _statsMap = new HashMap<String, Stat>();
    private final List<Stat> _statsList = new ArrayList<Stat>();

    enum Display {
        TOTAL, CHANGE, RATE
    };

    static class Stat {
        final String _name;
        long _time;
        long _value;
        long _interval;
        long _change;

        Stat(final String name) {
            this._name = name;
        }

        final void update(final long time, final long value) {
            _interval = time - _time;
            _change = value - _value;
            _time = time;
            _value = value;
        }

        @Override
        public String toString() {
            return toString(Display.TOTAL);
        }

        public String toString(final Display display) {
            switch (display) {
            case TOTAL:
                return String.format(COUNT_FORMAT, _name, _value);
            case CHANGE:
                return String.format(COUNT_FORMAT, _name, _change);
            case RATE:
                return String.format(RATE_FORMAT, _name, rate());
            default:
                throw new IllegalStateException();
            }
        }

        public void printHeader(final PrintWriter pw) throws IOException {
            pw.print(String.format(PHEADER_FORMAT, _name));
        }

        public void printValue(final PrintWriter pw, final Display display) throws IOException {
            switch (display) {
            case TOTAL:
                pw.print(String.format(PCOUNT_FORMAT, _value));
                break;
            case CHANGE:
                pw.print(String.format(PCOUNT_FORMAT, _change));
                break;
            case RATE:
                pw.print(String.format(PRATE_FORMAT, rate()));
                break;
            default:
                throw new IllegalStateException();
            }
        }

        public double rate() {
            if (_interval > 0) {
                return ((double) _change * NANOS_PER_SECOND) / _interval;
            }
            return -1.0d;
        }
    }

    @Cmd("stat")
    static Task createStatisticsTask(
            @Arg("delay|long:10:0:10000000|Interval in seconds between updates") final long delay,
            @Arg("count|long:1:0:|Number of updates") final long count,
            @Arg("file|string|Output file name") final String toFile, @Arg("_flag|a|All") final boolean all,
            @Arg("_flag|b|Buffer pool statistics") final boolean bstats,
            @Arg("_flag|j|Journal statistics") final boolean jstats,
            @Arg("_flag|i|I/O Statistics") final boolean istats,
            @Arg("_flag|t|Transaction statistics") final boolean tstats,
            @Arg("_flag|r|Show rates") final boolean showRates, @Arg("_flag|v|Show values") final boolean showValues)
            throws Exception {
        final StatisticsTask task = new StatisticsTask();
        task._delay = delay;
        task._count = count;
        task._fileName = toFile;
        task._bpool = bstats || all;
        task._journal = jstats || all;
        task._io = istats || all;
        task._transactions = tstats || all;
        task._showRate = showRates;
        task._showValue = showValues;
        return task;
    }

    @Override
    protected void runTask() throws Exception {
        if (_fileName != null && !_fileName.isEmpty()) {
            _pw = new PrintWriter(new FileOutputStream(_fileName, true));
        }
        if (!_bpool && !_journal && !_transactions && !_io) {
            postMessage("No statistics selected", 0);
            return;
        }

        final StringBuilder sb = new StringBuilder();

        long now = System.nanoTime();
        long next = now;

        boolean first = true;
        for (long count = 0; count < _count || count == 0; count++) {
            if (count != 0) {
                next += (_delay * NANOS_PER_SECOND);
                while (true) {
                    poll();
                    now = System.nanoTime();
                    final long sleep = Math.min(1000, (next - now) / NANOS_PER_MILLI);
                    if (sleep <= 0) {
                        break;
                    }
                    Util.sleep(sleep);
                }
            }
            updateStatistics(now);
            sb.setLength(0);
            final Display d = _showRate ? Display.RATE : count == 0 ? Display.TOTAL : Display.CHANGE;
            if (count > 0 || !_showRate) {
                for (final Stat stat : _statsList) {
                    if (sb.length() > 0) {
                        sb.append(' ');
                    }
                    sb.append(stat.toString(d));
                }
                sb.append(' ');
                sb.append(d);
                final String line = sb.toString();
                _lastUpdate = line;
                if (_pw != null) {
                    if (first) {
                        _pw.printf(TIME_HEADER_FORMAT, "elapsed ms");
                        for (final Stat stat : _statsList) {
                            stat.printHeader(_pw);
                        }
                        _pw.println();
                        first = false;
                    }
                    _pw.printf(TIME_FORMAT, _persistit.elapsedTime());
                    for (final Stat stat : _statsList) {
                        stat.printValue(_pw, d);
                    }
                    _pw.print(' ');
                    _pw.println(d.toString());
                    _pw.flush();
                } else {
                    postMessage(line, 1);
                }
            }
            if (_delay < 1) {
                break;
            }
        }
    }

    private Stat stat(final String name) {
        Stat stat = _statsMap.get(name);
        if (stat == null) {
            stat = new Stat(name);
            _statsMap.put(name, stat);
            _statsList.add(stat);
        }
        return stat;
    }

    private void updateStatistics(final long time) throws Exception {
        final Management management = _persistit.getManagement();
        if (_bpool) {
            updateBPoolStatistics(management, time);
        }
        if (_journal) {
            updateJournalStatistics(management, time);
        }
        if (_transactions) {
            updateTransactionStatistics(management, time);
        }
        if (_io) {
            updateIOStatistics(management, time);
        }
    }

    private void updateBPoolStatistics(final Management management, final long time) throws Exception {
        final BufferPoolInfo[] array = management.getBufferPoolInfoArray();
        long hits = 0;
        long misses = 0;
        long newPages = 0;
        long evictions = 0;
        for (final BufferPoolInfo info : array) {
            hits += info.getHitCount();
            misses += info.getMissCount();
            newPages += info.getNewCount();
            evictions += info.getEvictCount();
        }
        stat("hit").update(time, hits);
        stat("miss").update(time, misses);
        stat("new").update(time, newPages);
        stat("evict").update(time, evictions);
    }

    private void updateJournalStatistics(final Management management, final long time) throws Exception {
        final JournalInfo info = management.getJournalInfo();
        stat("jwrite").update(time, info.getJournaledPageCount());
        stat("jread").update(time, info.getReadPageCount());
        stat("jcopy").update(time, info.getCopiedPageCount());
    }

    private void updateTransactionStatistics(final Management management, final long time) throws Exception {
        final TransactionInfo info = management.getTransactionInfo();
        stat("tcommit").update(time, info.getCommitCount());
        stat("troll").update(time, info.getRollbackCount());
    }

    private void updateIOStatistics(final Management management, final long time) throws Exception {
        final IOMeterMXBean ioMeter = _persistit.getIOMeter();
        final String[] items = IOMeterMXBean.SUMMARY_ITEMS;
        long size = 0;

        for (final String item : items) {
            stat(item).update(time, ioMeter.totalOperations(item));
            size += ioMeter.totalBytes(item);
        }
        stat("IOkbytes").update(time, (size + 600) / 1000);
    }

    @Override
    public String getStatus() {
        return _lastUpdate;
    }

}
