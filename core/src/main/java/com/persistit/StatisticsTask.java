/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
import com.persistit.util.Util;

/**
 * Task that reports, either once or periodically, various runtime statistics.
 * 
 * @author peter
 */
public class StatisticsTask extends Task {

    static final long NANOS_PER_MILLI = 1000000;
    static final long NANOS_PER_SECOND = 1000000000;

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

    private Map<String, Stat> _statsMap = new HashMap<String, Stat>();
    private List<Stat> _statsList = new ArrayList<Stat>();

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

        public String toString(Display display) {
            switch (display) {
            case TOTAL:
                return String.format("%s=%d", _name, _value);
            case CHANGE:
                return String.format("%s=%d", _name, _change);
            case RATE:
                return String.format("%s=%.3f", _name, rate());
            default:
                throw new IllegalStateException();
            }
        }

        public void printHeader(final PrintWriter pw) throws IOException {
            pw.print(String.format(" %10s", _name));
        }

        public void printValue(final PrintWriter pw, final Display display) throws IOException {
            switch (display) {
            case TOTAL:
                pw.print(String.format(" %,10d", _value));
                break;
            case CHANGE:
                pw.print(String.format(" %,10d", _change));
                break;
            case RATE:
                pw.print(String.format(" %,10.3f", rate()));
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
    static Task createStatisticsTask(@Arg("delay|long:10:0:10000000|Interval in seconds between updates") long delay,
            @Arg("count|long:1:0:|Number of updates") long count, @Arg("file|string|Output file name") String toFile,
            @Arg("_flag|a|All") boolean all, @Arg("_flag|b|Buffer pool statistics") boolean bstats,
            @Arg("_flag|j|Journal statistics") boolean jstats, @Arg("_flag|i|I/O Statistics") boolean istats,
            @Arg("_flag|t|Transaction statistics") boolean tstats, @Arg("_flag|r|Show rates") boolean showRates,
            @Arg("_flag|v|Show values") boolean showValues) throws Exception {
        StatisticsTask task = new StatisticsTask();
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
                    long sleep = Math.min(1000, (next - now) / NANOS_PER_MILLI);
                    if (sleep <= 0) {
                        break;
                    }
                    Util.sleep(sleep);
                }
            }
            updateStatistics(now);
            sb.setLength(0);
            Display d = _showRate ? Display.RATE : count == 0 ? Display.TOTAL : Display.CHANGE;
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
                        _pw.printf("%12s ", "elapsed ms");
                        for (final Stat stat : _statsList) {
                            stat.printHeader(_pw);
                        }
                        _pw.println();
                        first = false;
                    }
                    _pw.printf("%,12d ", _persistit.elapsedTime());
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

        for (String item : items) {
            stat(item).update(time, ioMeter.getCount(item));
            size += ioMeter.getSum(item);
        }
        stat("IOkbytes").update(time, (size + 600) / 1000);
    }

    @Override
    public String getStatus() {
        return _lastUpdate;
    }

}
