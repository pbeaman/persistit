package com.persistit;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class TimestampAllocator {

    /**
     * Default interval in nanoseconds between checkpoints - five minutes.
     */
    private final static long DEFAULT_CHECKPOINT_INTERVAL = 60000000000L;

    private final AtomicLong _timestamp = new AtomicLong();

    private Checkpoint _checkpoint = new Checkpoint(0, 0);

    private long _lastCheckpointNanos;

    private volatile long _checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    public long updateTimestamp() {
        return _timestamp.incrementAndGet();
    }

    public long updateTimestamp(final long timestamp) {
        _timestamp.incrementAndGet();
        while (true) {
            final long expected = _timestamp.get();
            if (expected < timestamp) {
                if (_timestamp.compareAndSet(expected, timestamp)) {
                    return timestamp;
                }
            } else {
                return expected;
            }
        }
    }

    public long getCurrentTimestamp() {
        return _timestamp.get();
    }

    public synchronized Checkpoint updateCheckpoint() {
        final long now = System.nanoTime();
        if (_lastCheckpointNanos + _checkpointInterval < now) {
            _lastCheckpointNanos = now;
            return forceCheckpoint();
        } else {
            return _checkpoint;
        }
    }

    public synchronized Checkpoint forceCheckpoint() {
        final long checkpointTimestamp = _timestamp.addAndGet(10000000);
        _checkpoint = new Checkpoint(checkpointTimestamp, System
                .currentTimeMillis());
        return _checkpoint;
    }

    public static class Checkpoint {

        private final static SimpleDateFormat SDF = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");

        private final long _timestamp;

        private final long _systemTime;

        public Checkpoint(final long timestamp, final long systemTime) {
            _timestamp = timestamp;
            _systemTime = systemTime;
        }

        public long getTimestamp() {
            return _timestamp;
        }

        public long getSystemTimeMillis() {
            return _systemTime;
        }

        public String toString() {
            return String.format("Checkpoint %,d @ %s", _timestamp, SDF
                    .format(new Date(_systemTime)));
        }

    }

    public synchronized Checkpoint getCurrentCheckpoint() {
        return _checkpoint;
    }

    public long getCheckpointInterval() {
        return _checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        _checkpointInterval = checkpointInterval;
    }

}
