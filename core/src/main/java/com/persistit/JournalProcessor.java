/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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
