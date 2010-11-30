package com.persistit;

import com.persistit.exception.PersistitIOException;

public interface JournalProcessor {

    void je(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void iv(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void it(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void pa(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void pm(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void tm(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void ts(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void tc(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void tr(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void cp(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void jh(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void sr(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void dr(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

    void dt(final long from, final long timestamp, final int recordSize)
            throws PersistitIOException;

}
