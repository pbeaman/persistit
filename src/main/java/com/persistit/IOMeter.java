package com.persistit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Attempt to meter I/O operations so that background I/O-intensive processes
 * (e.g., the JournalManager's JOURNAL_COPIER thread) can voluntarily throttle
 * I/O consumption.
 * 
 * @author peter
 * 
 */
public class IOMeter {

    private static final String DUMP_FORMAT = "time=%,12d op=%2s vol=%4s page=%,16d addr=%,16d size=%,8d";

    private final static long NANOS_TO_MILLIS = 1000000l;
    private final static long DEFAULT_TICK_SIZE = 500 * NANOS_TO_MILLIS;
    private final static int DEFAULT_QUIESCENT_IO_THRESHOLD = 100000;
    private final static long DEFAULT_COPY_SLEEP_INTERVAL = 50;

    private final static int COPY_PAGE_TO_VOLUME = 1;
    private final static int READ_PAGE_FROM_VOLUME = 2;
    private final static int WRITE_PAGE_TO_VOLUME = 3;
    private final static int READ_PAGE_FROM_JOURNAL = 4;
    private final static int WRITE_PAGE_TO_JOURNAL = 5;
    private final static int WRITE_TS_TO_JOURNAL = 6;
    private final static int WRITE_TC_TO_JOURNAL = 7;
    private final static int WRITE_SR_TO_JOURNAL = 8;
    private final static int WRITE_DR_TO_JOURNAL = 9;
    private final static int WRITE_DT_TO_JOURNAL = 10;
    private final static int WRITE_OTHER_TO_JOURNAL = 11;

    private final static String[] OPERATIONS = { "??", "CC", "RV", "WV", "RJ",
            "WJ", "TS", "TC", "SR", "DR", "DT", "XX" };

    private AtomicReference<DataOutputStream> _logStream = new AtomicReference<DataOutputStream>();

    private long _ioRate;

    private long _lastTick;

    private long _tickSize = DEFAULT_TICK_SIZE;

    private long _copySleepInterval = DEFAULT_COPY_SLEEP_INTERVAL;

    private long _quiescentIO = DEFAULT_QUIESCENT_IO_THRESHOLD;

    /**
     * Time interval in milliseconds between page copy operations.
     * 
     * @return the CopySleepInterval
     */
    public synchronized long getCopySleepInterval() {
        return _copySleepInterval;
    }

    /**
     * @param _copySleepInterval
     *            the _copySleepInterval to set
     */
    public synchronized void setCopySleepInterval(long copySleepInterval) {
        _copySleepInterval = copySleepInterval;
    }

    /**
     * @return the _quiescentIO
     */
    public synchronized long getQuiescentIOthreshold() {
        return _quiescentIO;
    }

    /**
     * @param _quiescentIO
     *            the _quiescentIO to set
     */
    public void setQuiescentIOthreshold(long quiescentIO) {
        _quiescentIO = quiescentIO;
    }

    /**
     * @return the _ioRate
     */
    public long getIoRate() {
        return _ioRate;
    }

    public void setLogFile(final File toFile) throws IOException {
        if (toFile == null) {
            final DataOutputStream dos = _logStream.get();
            if (dos != null) {
                _logStream.set(null);
                dos.close();
            }
        } else if (_logStream.get() == null) {
            _logStream.set(new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(toFile))));
        }
    }

    private synchronized void charge(final long time, final int size) {
        final long ticks = (time - _lastTick) / _tickSize;
        if (ticks > 32) {
            _ioRate = 0;
        } else {
            _ioRate >>>= ticks;
        }
        _lastTick = (time / _tickSize) * _tickSize;
        _ioRate += size;
    }

    private synchronized void log(int type, final long time,
            final Volume volume, long pageAddress, int size, long journalAddress) {
        final DataOutputStream os = _logStream.get();
        if (os != null) {
            try {
                os.write((byte) type);
                os.writeLong(time);
                os.writeInt(volume == null ? 0 : (int) volume.getId()
                        ^ (int) (volume.getId() >>> 32));
                os.writeLong(pageAddress);
                os.writeInt(size);
                os.writeLong(journalAddress);
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private void dump(final DataInputStream is) throws IOException {
        long start = 0;
        final Map<Integer, Integer> volMap = new HashMap<Integer, Integer>();
        int counter = 0;
        while (true) {
            try {
                final int type = is.read() & 0xFF;
                final String opName = type >= 0 && type < OPERATIONS.length ? OPERATIONS[type]
                        : "??";
                final long time = is.readLong() / NANOS_TO_MILLIS;
                final int volumeHash = is.readInt();
                final long pageAddress = is.readLong();
                final int size = is.readInt();
                final long journalAddress = is.readLong();
                if (start == 0) {
                    start = time;
                }
                Integer volumeId = volMap.get(volumeHash);
                if (volumeId == null) {
                    volumeId = Integer.valueOf(++counter);
                    volMap.put(volumeHash, volumeId);
                }
                System.out.println(String.format(DUMP_FORMAT, time - start,
                        opName, volumeId, pageAddress, journalAddress, size));

            } catch (EOFException e) {
                // normal end of processing
            }
        }
    }

    public void chargeCopyPageToVolume(final Volume volume,
            final long pageAddress, final int size, final long journalAddress,
            final boolean urgent) {
        final long time = System.nanoTime();
        log(COPY_PAGE_TO_VOLUME, time, volume, pageAddress, size,
                journalAddress);

        // Determine how long to sleep. This logic attempts to slow down
        // page copy operations when there are a lot of other I/O operations
        // going on concurrently. But when the ioRate falls below the
        // _quiecentIO threshold, it returns immediately without sleeping.
        long sleep = 0;
        if (!urgent) {
            synchronized (this) {
                sleep = _ioRate > _quiescentIO ? _copySleepInterval : 0;
            }
        }
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    public void chargeReadPageFromVolume(final Volume volume,
            final long pageAddress, final int size) {
        final long time = System.nanoTime();
        log(READ_PAGE_FROM_VOLUME, time, volume, pageAddress, size, -1);
        charge(time, size);
    }

    public void chargeWritePageToVolume(final Volume volume,
            final long pageAddress, final int size) {
        final long time = System.nanoTime();
        log(WRITE_PAGE_TO_VOLUME, time, volume, pageAddress, size, -1);
        charge(time, size);
    }

    public void chargeReadPageFromJournal(final Volume volume,
            final long pageAddress, final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(READ_PAGE_FROM_JOURNAL, time, volume, pageAddress, size,
                journalAddress);
        charge(time, size);
    }

    public void chargeWritePageToJournal(final Volume volume,
            final long pageAddress, final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_PAGE_TO_JOURNAL, time, volume, pageAddress, size,
                journalAddress);
        charge(time, size);
    }

    public void chargeWriteTStoJournal(final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_TS_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public void chargeWriteTCtoJournal(final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_TC_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public void chargeWriteSRtoJournal(final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_SR_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public void chargeWriteDRtoJournal(final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_DR_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public void chargeWriteDTtoJournal(final int size, final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_DT_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public void chargeWriteOtherToJournal(final int size,
            final long journalAddress) {
        final long time = System.nanoTime();
        log(WRITE_OTHER_TO_JOURNAL, time, null, -1, size, journalAddress);
        charge(time, size);
    }

    public static void main(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("com.persistit.IOMeter", args,
                new String[] { "file||log file name" });
        final String fileName = ap.getStringValue("file");
        if (fileName == null) {
            ap.usage();
        } else {
            IOMeter ioMeter = new IOMeter();
            final DataInputStream is = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(fileName)));
            ioMeter.dump(is);
            is.close();
        }
    }
}
