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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.mxbeans.IOMeterMXBean;
import com.persistit.util.ArgParser;

/**
 * 
 * This class accumulates statistics on file system I/O operations. Each I/O
 * operation is "charged" to a bucket according to the operation being performed
 * (see {@link IOMeterMXBean#OPERATIONS}).
 * <p />
 * Statistics are kept for recent time intervals so that Persistit can compute
 * I/O rates for the recent past. These are kept in "buckets" with a granularity
 * of about 1 second. Persistit examines the recent I/O history when scheduling
 * the journal copying operation.
 * 
 * @author peter
 * 
 */
class IOMeter implements IOMeterMXBean {

    private final static int BUCKETS = 5;
    private final static long SECOND = 1000000000L;
    private final static long RECENT = 3 * SECOND;
    private final static long KILO = 1024;

    private final static String DUMP_FORMAT = "time=%,12d op=%2s vol=%4s page=%,16d addr=%,16d size=%,8d index=%,7d";
    private final static int DUMP_RECORD_LENGTH = 37;

    private final static int DEFAULT_QUIESCENT_IO_THRESHOLD = 100000;
    private final static long DEFAULT_COPY_PAGE_SLEEP_INTERVAL = 10000;
    private final static long DEFAULT_WRITE_PAGE_SLEEP_INTERVAL = 0;

    private final static int READ_PAGE_FROM_VOLUME = 1;
    private final static int READ_PAGE_FROM_JOURNAL = 2;
    private final static int COPY_PAGE_TO_VOLUME = 3;
    private final static int WRITE_PAGE_TO_JOURNAL = 4;
    private final static int WRITE_TS_TO_JOURNAL = 5;
    private final static int WRITE_TC_TO_JOURNAL = 6;
    private final static int WRITE_SR_TO_JOURNAL = 7;
    private final static int WRITE_DR_TO_JOURNAL = 8;
    private final static int WRITE_DT_TO_JOURNAL = 9;
    private final static int WRITE_OTHER_TO_JOURNAL = 10;
    private final static int EVICT_PAGE_FROM_POOL = 11;
    private final static int FLUSH_JOURNAL = 12;
    private final static int GET_PAGE = 13;

    private final static int ITEM_COUNT = 14;

    private long _copyPageSleepInterval = DEFAULT_COPY_PAGE_SLEEP_INTERVAL;

    private long _writePageSleepInterval = DEFAULT_WRITE_PAGE_SLEEP_INTERVAL;

    private long _quiescentIOthreshold = DEFAULT_QUIESCENT_IO_THRESHOLD;

    private AtomicReference<DataOutputStream> _logStream = new AtomicReference<DataOutputStream>();

    private String _logFileName;

    private Counter[][] _counters = new Counter[ITEM_COUNT][BUCKETS];

    private long[] _clockTimes = new long[BUCKETS];

    private AtomicLong[] _totalCounts = new AtomicLong[ITEM_COUNT];
    private AtomicLong[] _totalSums = new AtomicLong[ITEM_COUNT];

    volatile int _currentBucket;

    private static class Counter {
        AtomicLong _count = new AtomicLong();
        AtomicLong _sum = new AtomicLong();

        void reset() {
            _count.set(0);
            _sum.set(0);
        }

        void charge(long size) {
            _count.incrementAndGet();
            _sum.addAndGet(size);
        }

        long sum() {
            return _sum.get();
        }

        long count() {
            return _count.get();
        }

        public String toString() {
            return String.format("(%d:%d)", _count.get(), _sum.get());
        }
    }

    IOMeter() {
        for (int item = 0; item < ITEM_COUNT; item++) {
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                _counters[item][bucket] = new Counter();
            }
            _totalCounts[item] = new AtomicLong();
            _totalSums[item] = new AtomicLong();
        }
        _clockTimes[_currentBucket] = System.nanoTime();
    }

    /**
     * Called periodically to note the passage of time. If a second has passed
     * since the last call, then this method advances the bucket index, and
     * clears the associated bucket. Statistics will be gathered in that bucket
     * until the next time the bucket index is incremented.
     */
    synchronized void poll() {
        int bucket = _currentBucket;
        final long now = System.nanoTime();
        if (now - _clockTimes[bucket] > SECOND) {
            bucket = (bucket + 1) % BUCKETS;
            _clockTimes[bucket] = now;
            for (int item = 0; item < ITEM_COUNT; item++) {
                _totalCounts[item].addAndGet(_counters[item][bucket].count());
                _totalSums[item].addAndGet(_counters[item][bucket].sum());
                _counters[item][bucket].reset();
            }
            _currentBucket = bucket;
        }
    }

    /**
     * @return the writePageSleepInterval
     */
    @Override
    public synchronized long getWritePageSleepInterval() {
        return _writePageSleepInterval;
    }

    /**
     * @param writePageSleepInterval
     *            the writePageSleepInterval to set
     */
    @Override
    public synchronized void setWritePageSleepInterval(long writePageSleepInterval) {
        _writePageSleepInterval = writePageSleepInterval;
    }

    /**
     * Time interval in milliseconds between page copy operations.
     * 
     * @return the CopySleepInterval
     */
    @Override
    public synchronized long getCopyPageSleepInterval() {
        return _copyPageSleepInterval;
    }

    /**
     * @param copyPageSleepInterval
     *            the copySleepInterval to set
     */
    @Override
    public synchronized void setCopyPageSleepInterval(long copyPageSleepInterval) {
        _copyPageSleepInterval = copyPageSleepInterval;
    }

    /**
     * @return the quiescentIOthreshold
     */
    @Override
    public synchronized long getQuiescentIOthreshold() {
        return _quiescentIOthreshold;
    }

    /**
     * @param quiescentIOthreshold
     *            the quiescentIOthreshold to set
     */
    @Override
    public synchronized void setQuiescentIOthreshold(long quiescentIO) {
        _quiescentIOthreshold = quiescentIO;
    }

    /**
     * @return the ioRate
     */
    @Override
    public synchronized long getIoRate() {
        int bucket = _currentBucket;
        int previousBucket = (bucket + BUCKETS - 1) % BUCKETS;
        long interval = _clockTimes[bucket] - _clockTimes[previousBucket];
        if (interval < SECOND) {
            return 0;
        }
        long sum = 0;
        for (int item = 0; item < ITEM_COUNT; item++) {
            sum += _counters[item][previousBucket].sum();
        }
        return (sum * SECOND / interval) / KILO;
    }

    @Override
    public synchronized void setLogFile(final String toFile) throws IOException {
        if (toFile == null || toFile.isEmpty()) {
            final DataOutputStream dos = _logStream.get();
            if (dos != null) {
                _logStream.set(null);
                dos.close();
            }
        } else if (_logStream.get() == null) {
            _logStream.set(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toFile))));
        }
        _logFileName = toFile;
    }

    @Override
    public String getLogFile() {
        return _logFileName;
    }

    private void charge(final int size, int item) {
        int bucket = _currentBucket;
        _counters[item][bucket].charge(size);
    }

    private void log(int type, final Volume volume, long pageAddress, int size, long journalAddress, int bufferIndex) {
        final DataOutputStream os = _logStream.get();
        if (os != null) {
            synchronized (os) {
                try {
                    os.write((byte) type);
                    os.writeLong(System.currentTimeMillis());
                    os.writeInt(volume == null ? 0 : volume.getHandle());
                    os.writeLong(pageAddress);
                    os.writeInt(size);
                    os.writeLong(journalAddress);
                    os.writeInt(bufferIndex);
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private static class Event {
        long _time;
        int _count;
        int _op;

        Event(long time, int count, int op) {
            _time = time;
            _count = count;
            _op = op;
        }

        private String describe(final long time, final int count) {
            final String opName = _op >= 0 && _op < OPERATIONS.length ? OPERATIONS[_op] : "??";
            return String.format("%s %,10dms %,8d events ago", opName, time - _time, count - _count);
        }
    }

    private void dump(final DataInputStream is, int count, boolean analyzePages) throws IOException {
        long start = 0;
        Map<Long, List<Event>> events = new HashMap<Long, List<Event>>();
        for (int index = 0; index < count; index++) {
            try {
                final int op = is.read();
                if (op == -1) {
                    break;
                }
                final String opName = op >= 0 && op < OPERATIONS.length ? OPERATIONS[op] : "??";
                final long time = is.readLong();
                final int volumeHandle = is.readInt();
                final long pageAddress = is.readLong();
                final int size = is.readInt();
                final long journalAddress = is.readLong();
                final int bufferIndex = is.readInt();
                if (start == 0) {
                    start = time;
                }
                System.out.printf(DUMP_FORMAT, time - start, opName, volumeHandle, pageAddress, journalAddress, size,
                        bufferIndex);

                if (analyzePages
                        && (op == WRITE_PAGE_TO_JOURNAL || op == READ_PAGE_FROM_JOURNAL || op == READ_PAGE_FROM_VOLUME
                                || op == COPY_PAGE_TO_VOLUME || op == EVICT_PAGE_FROM_POOL)) {
                    long handle = (volumeHandle << 48) + pageAddress;
                    List<Event> list = events.get(handle);
                    if (list == null) {
                        list = new ArrayList<Event>(2);
                        events.put(handle, list);
                    }
                    for (Event e : list) {
                        System.out.printf("  %-35s", e.describe(time, index));
                    }
                    while (list.size() >= 2) {
                        list.remove(0);
                    }
                    list.add(new Event(time, index, op));
                }

                System.out.println();

            } catch (EOFException e) {
                break;
                // normal end of processing
            }
        }
    }

    public void chargeCopyPageToVolume(final Volume volume, final long pageAddress, final int size,
            final long journalAddress, final int urgency) {
        charge(size, COPY_PAGE_TO_VOLUME);
        log(COPY_PAGE_TO_VOLUME, volume, pageAddress, size, journalAddress, 0);
    }

    public void chargeReadPageFromVolume(final Volume volume, final long pageAddress, final int size, int bufferIndex) {
        log(READ_PAGE_FROM_VOLUME, volume, pageAddress, size, -1, bufferIndex);
        charge(size, READ_PAGE_FROM_VOLUME);
    }

    public void chargeReadPageFromJournal(final Volume volume, final long pageAddress, final int size,
            final long journalAddress, int bufferIndex) {
        log(READ_PAGE_FROM_JOURNAL, volume, pageAddress, size, journalAddress, bufferIndex);
        charge(size, READ_PAGE_FROM_JOURNAL);
    }

    public void chargeWritePageToJournal(final Volume volume, final long pageAddress, final int size,
            final long journalAddress, final int urgency, int bufferIndex) {
        log(WRITE_PAGE_TO_JOURNAL, volume, pageAddress, size, journalAddress, bufferIndex);
        charge(size, WRITE_PAGE_TO_JOURNAL);
    }

    public void chargeWriteTStoJournal(final int size, final long journalAddress) {
        log(WRITE_TS_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_TS_TO_JOURNAL);
    }

    public void chargeWriteTCtoJournal(final int size, final long journalAddress) {
        log(WRITE_TC_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_TC_TO_JOURNAL);
    }

    public void chargeWriteSRtoJournal(final int size, final long journalAddress) {
        log(WRITE_SR_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_SR_TO_JOURNAL);
    }

    public void chargeWriteDRtoJournal(final int size, final long journalAddress) {
        log(WRITE_DR_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_DR_TO_JOURNAL);
    }

    public void chargeWriteDTtoJournal(final int size, final long journalAddress) {
        log(WRITE_DT_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_DT_TO_JOURNAL);
    }

    public void chargeWriteOtherToJournal(final int size, final long journalAddress) {
        log(WRITE_OTHER_TO_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, WRITE_OTHER_TO_JOURNAL);
    }

    public void chargeEvictPageFromPool(final Volume volume, final long pageAddress, final int size, int bufferIndex) {
        log(EVICT_PAGE_FROM_POOL, volume, pageAddress, size, 0, bufferIndex);
    }

    public void chargeFlushJournal(final int size, final long journalAddress) {
        log(FLUSH_JOURNAL, null, -1, size, journalAddress, -1);
        charge(size, FLUSH_JOURNAL);
    }

    public void chargeGetPage(final Volume volume, final long pageAddress, final int size, int bufferIndex) {
        log(GET_PAGE, volume, pageAddress, size, 0, bufferIndex);
    }

    @Override
    public long getCount(final String opName) {
        return getCount(op(opName));
    }

    public long getCount(final int op) {
        if (op > 0 && op < ITEM_COUNT) {
            long count = _totalCounts[op].get();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                count += _counters[op][bucket].count();
            }
            return count;
        } else {
            return -1;
        }
    }

    @Override
    public long getSum(final String opName) {
        return getSum(op(opName));
    }

    public long getSum(final int op) {
        if (op > 0 && op < ITEM_COUNT) {
            long sum = _totalSums[op].get();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                sum += _counters[op][bucket].sum();
            }
            return sum;
        } else {
            return -1;
        }
    }

    public int op(final String opName) {
        for (int index = 1; index < ITEM_COUNT; index++) {
            if (OPERATIONS[index].equalsIgnoreCase(opName)) {
                return index;
            }
        }
        return -1;
    }

    /**
     * Returns an estimate of the number of bytes read or written per second
     * during a recent time interval. This measurement does not include journal
     * copying because this method is used by the journal copier to determine
     * how frequently to schedule its own activities without interfering
     * severely with current operational load.
     * 
     * @return
     */
    synchronized long recentCharge() {
        long now = System.nanoTime();
        long then = 0;
        int current = _currentBucket;
        long charge = 0;
        for (int b = current + BUCKETS; b > current; b--) {
            int bucket = b % BUCKETS;
            if (_clockTimes[bucket] < now - RECENT) {
                break;
            }
            for (int item = 0; item < ITEM_COUNT; item++) {
                if (item != COPY_PAGE_TO_VOLUME) {
                    charge += _counters[item][bucket].sum();
                }
            }
            then = _clockTimes[bucket];
        }
        if (now - then <= 0) {
            return -1;
        }
        return charge * SECOND / (now - then);
    }

    /**
     * Dump an IOMeter log file to System.out. For diagnostic purposes only.
     * 
     * @param args
     *            Specify one argument in the form file=<pathname>
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("com.persistit.IOMeter", args, new String[] { "file||log file name",
                "skip|long:0:0:1000000000000|event skip count", "count|long:0:0:2000000000|event count nlimit",
                "_flag|a|Analyze page pattern" });
        final String fileName = ap.getStringValue("file");
        final long skip = ap.getLongValue("skip");
        final int count = ap.getIntValue("count");
        if (fileName == null) {
            ap.usage();
        } else {
            IOMeter ioMeter = new IOMeter();
            final DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
            is.skip(skip * DUMP_RECORD_LENGTH);
            ioMeter.dump(is, count == 0 ? Integer.MAX_VALUE : count, ap.isFlag('a'));
            is.close();
        }
    }
}
