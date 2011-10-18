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

import java.util.List;

import com.persistit.TransactionalCache.Update;
import com.persistit.exception.PersistitIOException;

interface TransactionWriter {

    boolean writeTransactionStartToJournal(final long startTimestamp) throws PersistitIOException;

    boolean writeTransactionCommitToJournal(final long timestamp, final long commitTimestamp)
            throws PersistitIOException;

    boolean writeStoreRecordToJournal(final long timestamp, final int treeHandle, final Key key, final Value value)
            throws PersistitIOException;

    boolean writeDeleteRecordToJournal(final long timestamp, final int treeHandle, final Key key1, final Key key2)
            throws PersistitIOException;

    boolean writeDeleteTreeToJournal(final long timestamp, final int treeHandle) throws PersistitIOException;

    boolean writeCacheUpdatesToJournal(final long timestamp, final long cacheId, final List<Update> udpates)
            throws PersistitIOException;
}
