/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

/**
 * 
 * Read and apply transaction from the journal to the live database. To apply
 * a transaction, this class calls methods of a 
 */
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.persistit.JournalManager.TransactionMapItem;
import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.JournalRecord.D0;
import com.persistit.JournalRecord.D1;
import com.persistit.JournalRecord.DR;
import com.persistit.JournalRecord.DT;
import com.persistit.JournalRecord.SR;
import com.persistit.JournalRecord.TX;
import com.persistit.exception.CorruptJournalException;
import com.persistit.exception.PersistitException;

class TransactionPlayer {

    interface TransactionPlayerListener {

        void startRecovery(long address, long timestamp) throws PersistitException;

        void startTransaction(long address, long timestamp, long commitTimestamp) throws PersistitException;

        void store(long address, long timestamp, Exchange exchange) throws PersistitException;

        void removeKeyRange(long address, long startTimestamp, Exchange exchange, Key from, Key to)
                throws PersistitException;

        void removeTree(long address, long timestamp, Exchange exchange) throws PersistitException;

        void delta(long address, long timestamp, Tree tree, int index, int accumulatorType, long value)
                throws PersistitException;

        void endTransaction(long address, long timestamp) throws PersistitException;

        void endRecovery(long address, long timestamp) throws PersistitException;
        
        boolean requiresLongRecordConversion();

    }

    final TransactionPlayerSupport _support;

    TransactionPlayer(final TransactionPlayerSupport support) {
        _support = support;
    }

    public void applyTransaction(final TransactionMapItem item, final TransactionPlayerListener listener)
            throws PersistitException {

        final List<Long> chainedAddress = new ArrayList<Long>();
        long address = item.getLastRecordAddress();

        int recordSize;
        int type;
        long startTimestamp;
        long commitTimestamp;
        long backchainAddress;
        int appliedUpdates = 0;

        for (;;) {
            _support.read(address, TX.OVERHEAD);
            recordSize = TX.getLength(_support.getReadBuffer());
            _support.read(address, recordSize);
            type = TX.getType(_support.getReadBuffer());
            startTimestamp = TX.getTimestamp(_support.getReadBuffer());
            commitTimestamp = TX.getCommitTimestamp(_support.getReadBuffer());
            backchainAddress = TX.getBackchainAddress(_support.getReadBuffer());
            if (recordSize < TX.OVERHEAD || recordSize > Transaction.TRANSACTION_BUFFER_SIZE + TX.OVERHEAD
                    || type != TX.TYPE) {
                throw new CorruptJournalException("Transaction record at " + addressToString(address)
                        + " has invalid length " + recordSize + " or type " + type);
            }
            if (startTimestamp != item.getStartTimestamp()) {
                throw new CorruptJournalException("Transaction record at " + addressToString(address)
                        + " has an invalid start timestamp: " + startTimestamp);
            }
            if (backchainAddress == 0) {
                if (address != item.getStartAddress()) {
                    throw new CorruptJournalException("Transaction record at " + addressToString(address)
                            + " has an invalid start " + addressToString(item.getStartAddress()));
                }
                break;
            }
            chainedAddress.add(0, address);
            address = backchainAddress;
        }

        listener.startTransaction(address, startTimestamp, commitTimestamp);
        appliedUpdates += applyTransactionUpdates(_support.getReadBuffer(), address, recordSize, startTimestamp, commitTimestamp,
                listener);

        for (Long continuation : chainedAddress) {
            address = continuation.longValue();
            _support.read(address, TX.OVERHEAD);
            recordSize = TX.getLength(_support.getReadBuffer());
            if (recordSize < TX.OVERHEAD || recordSize > Transaction.TRANSACTION_BUFFER_SIZE + TX.OVERHEAD
                    || type != TX.TYPE) {
                throw new CorruptJournalException("Transaction record at " + addressToString(address)
                        + " has invalid length " + recordSize + " or type " + type);
            }
            _support.read(address, recordSize);
            appliedUpdates += applyTransactionUpdates(_support.getReadBuffer(), address, recordSize, startTimestamp, commitTimestamp,
                    listener);
        }
        listener.endTransaction(address, startTimestamp);

    }

    int applyTransactionUpdates(final ByteBuffer byteBuffer, final long address, final int recordSize,
            final long startTimestamp, final long commitTimestamp, final TransactionPlayerListener listener)
            throws PersistitException {
        ByteBuffer bb = byteBuffer;
        final int start = bb.position();
        int end = start + recordSize;
        int position = start + TX.OVERHEAD;
        int appliedUpdates = 0;

        while (position < end) {
            bb.position(position);
            final int innerSize = JournalRecord.getLength(bb);
            final int type = JournalRecord.getType(bb);
            switch (type) {
            case SR.TYPE: {
                final int keySize = SR.getKeySize(bb);
                final int treeHandle = SR.getTreeHandle(bb);
                final Exchange exchange = getExchange(treeHandle, address, startTimestamp);
                exchange.ignoreTransactions();
                final Key key = exchange.getKey();
                final Value value = exchange.getValue();
                System.arraycopy(bb.array(), bb.position() + SR.OVERHEAD, key.getEncodedBytes(), 0, keySize);
                key.setEncodedSize(keySize);
                final int valueSize = innerSize - SR.OVERHEAD - keySize;
                value.ensureFit(valueSize);
                System.arraycopy(bb.array(), bb.position() + SR.OVERHEAD + keySize, value.getEncodedBytes(), 0,
                        valueSize);
                value.setEncodedSize(valueSize);

                if (value.getEncodedSize() >= Buffer.LONGREC_SIZE
                        && (value.getEncodedBytes()[0] & 0xFF) == Buffer.LONGREC_TYPE) {
                    /*
                     * convertToLongRecord will pollute the getReadBuffer().
                     * Therefore before calling it we need to copy the TX record
                     * to a fresh ByteBuffer.
                     */
                    if (bb == _support.getReadBuffer()) {
                        end = recordSize - (position - start);
                        bb = ByteBuffer.allocate(end);
                        bb.put(_support.getReadBuffer().array(), position, end);
                        bb.flip();
                        position = 0;
                    }
                    if (listener.requiresLongRecordConversion()) {
                        _support.convertToLongRecord(value, treeHandle, address, commitTimestamp);
                    }
                }

                listener.store(address, startTimestamp, exchange);
                appliedUpdates++;
                // Don't keep exchanges with enlarged value - let them be GC'd
                if (exchange.getValue().getMaximumSize() < Value.DEFAULT_MAXIMUM_SIZE) {
                    releaseExchange(exchange);
                }
                break;
            }

            case DR.TYPE: {
                final int key1Size = DR.getKey1Size(bb);
                final int elisionCount = DR.getKey2Elision(bb);
                final Exchange exchange = getExchange(DR.getTreeHandle(bb), address, startTimestamp);
                exchange.ignoreTransactions();
                final Key key1 = exchange.getAuxiliaryKey1();
                final Key key2 = exchange.getAuxiliaryKey2();
                System.arraycopy(bb.array(), bb.position() + DR.OVERHEAD, key1.getEncodedBytes(), 0, key1Size);
                key1.setEncodedSize(key1Size);
                final int key2Size = innerSize - DR.OVERHEAD - key1Size;
                System.arraycopy(key1.getEncodedBytes(), 0, key2.getEncodedBytes(), 0, elisionCount);
                System.arraycopy(bb.array(), bb.position() + DR.OVERHEAD + key1Size, key2.getEncodedBytes(),
                        elisionCount, key2Size);
                key2.setEncodedSize(key2Size + elisionCount);
                listener.removeKeyRange(address, startTimestamp, exchange, exchange.getAuxiliaryKey1(), exchange
                        .getAuxiliaryKey2());
                appliedUpdates++;
                releaseExchange(exchange);
                break;
            }

            case DT.TYPE: {
                final Exchange exchange = getExchange(DT.getTreeHandle(bb), address, startTimestamp);
                listener.removeTree(address, startTimestamp, exchange);
                appliedUpdates++;
                releaseExchange(exchange);
                break;
            }

            case D0.TYPE: {
                final Exchange exchange = getExchange(D0.getTreeHandle(bb), address, startTimestamp);
                listener.delta(address, startTimestamp, exchange.getTree(), D0.getIndex(bb), D0
                        .getAccumulatorTypeOrdinal(bb), 1);
                appliedUpdates++;
                break;
            }

            case D1.TYPE: {
                final Exchange exchange = getExchange(D1.getTreeHandle(bb), address, startTimestamp);
                listener.delta(address, startTimestamp, exchange.getTree(), D1.getIndex(bb), D1
                        .getAccumulatorTypeOrdinal(bb), D1.getValue(bb));
                appliedUpdates++;
                break;
            }

            default: {
                throw new CorruptJournalException("Invalid record type " + type + " at journal address "
                        + addressToString(address + position - start) + " index of transaction record at "
                        + addressToString(address));
            }
            }
            position += innerSize;
        }
        return appliedUpdates;
    }

    public static String addressToString(final long address) {
        return String.format("JournalAddress %,d", address);
    }

    public static String addressToString(final long address, final long timestamp) {
        return String.format("JournalAddress %,d{%,d}", address, timestamp);
    }

    private Exchange getExchange(final int treeHandle, final long from, final long timestamp) throws PersistitException {
        final TreeDescriptor td = _support.handleToTreeDescriptor(treeHandle);
        if (td == null) {
            throw new CorruptJournalException("Tree handle " + treeHandle + " is undefined at "
                    + addressToString(from, timestamp));
        }
        Volume volume = _support.handleToVolume(td.getVolumeHandle());
        if (volume == null) {
            throw new CorruptJournalException("Volume handle " + td.getVolumeHandle() + " is undefined at "
                    + addressToString(from, timestamp));
        }

        if (!volume.isOpened()) {
            volume = _support.getPersistit().getVolume(volume.getName());
            if (volume == null) {
                throw new CorruptJournalException("No matching Volume found for journal reference " + volume + " at "
                        + addressToString(from, timestamp));
            }
        }
        volume.verifyId(volume.getId());

        if (VolumeStructure.DIRECTORY_TREE_NAME.equals(td.getTreeName())) {
            return volume.getStructure().directoryExchange();
        } else {
            return _support.getPersistit().getExchange(volume, td.getTreeName(), true);
        }
    }

    private void releaseExchange(final Exchange exchange) {
        _support.getPersistit().releaseExchange(exchange);
    }
}
