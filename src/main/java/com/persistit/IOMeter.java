package com.persistit;

/**
 * Attempt to meter I/O operations so that background I/O-intensive processes
 * (e.g., the JournalManager's JOURNAL_COPIER thread) can voluntarily throttle
 * I/O consumption.
 * 
 * @author peter
 * 
 */
public class IOMeter {

    private final static long IO_NANOSEC_PER_INTERVAL = 100000000L;

    private final static float IO_DECAY = 0.66f;

    private final static float IO_NORMALIZE = 100f / 27f;

    private final static int DEFAULT_IO_RATE_MAX = 10;

    private final static int DEFAULT_IO_RATE_MIN = 1;

    // TODO: Reconsider I/O scheduling
    //
    // Changed to ZERO (was 0.5f) to accelerate bulk load.  Suggests
    // that the whole idea of trying to throttle the JournalCopy
    // is bogus.  But for now I'm changing nothing but the constant.
    //
    private final static float DEFAULT_IO_RATE_SLEEP_MULTIPLIER = 0.0f;

    private int _ioRate;

    private long _ioTime;

    private volatile int _ioRateMin = DEFAULT_IO_RATE_MIN;

    private volatile int _ioRateMax = DEFAULT_IO_RATE_MAX;

    private volatile float _ioRateSleepMultiplier = DEFAULT_IO_RATE_SLEEP_MULTIPLIER;

    public int getRateIOMin() {
        return _ioRateMin;
    }

    public void setRateIOMin(int ioMin) {
        _ioRateMin = ioMin;
    }

    public int getRateIOMax() {
        return _ioRateMax;
    }

    public void setRateIOMax(int ioMax) {
        _ioRateMax = ioMax;
    }

    public int getIORate() {
        return _ioRate;
    }

    /**
     * Called at a steady rate of N operations per sec, the ioRate converges to
     * approximately N.
     */
    public synchronized int ioRate(final int delta) {
        final long now = System.nanoTime();
        final long elapsed = (now - _ioTime) / IO_NANOSEC_PER_INTERVAL;
        if (elapsed > 10) {
            _ioRate = 0;
        } else
            for (int i = (int) elapsed; --i >= 0;) {
                _ioRate *= IO_DECAY;
            }
        _ioRate += delta;
        _ioTime = now;
        return (int) (_ioRate * IO_NORMALIZE);
    }

    public void voluntaryWait(final boolean fast) {
        if (!fast) {
            final int ioRate = Math.min(Math.max(ioRate(0), _ioRateMin),
                    _ioRateMax);
            final long delay = (long) (_ioRateSleepMultiplier * ioRate);
            if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                // ignore
            }
            }
        }
    }
}
