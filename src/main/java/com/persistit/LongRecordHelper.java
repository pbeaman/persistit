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

import static com.persistit.Buffer.HEADER_SIZE;
import static com.persistit.Buffer.LONGREC_PREFIX_OFFSET;
import static com.persistit.Buffer.LONGREC_PREFIX_SIZE;
import static com.persistit.Buffer.LONGREC_SIZE;
import static com.persistit.Buffer.LONGREC_TYPE;
import static com.persistit.Buffer.MAX_LONG_RECORD_CHAIN;
import static com.persistit.Buffer.PAGE_TYPE_LONG_RECORD;
import static com.persistit.util.SequencerConstants.LONG_RECORD_ALLOCATE_A;
import static com.persistit.util.ThreadSequencer.sequence;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * @version 1.0
 */
class LongRecordHelper {

    final Persistit _persistit;
    final Volume _volume;
    final Exchange _exchange;

    LongRecordHelper(final Persistit persistit, final Volume volume) {
        _persistit = persistit;
        _volume = volume;
        _exchange = null;
    }

    LongRecordHelper(final Persistit persistit, final Exchange exchange) {
        _persistit = persistit;
        _volume = exchange.getVolume();
        _exchange = exchange;
    }

    /**
     * Decode the LONG_RECORD pointer that has previously been fetched into the
     * Value. This will replace the byte array in that value with the actual
     * long value.
     * 
     * @param value
     * @param minimumBytesToFetch
     * @throws PersistitException
     */
    void fetchLongRecord(Value value, int minimumBytesToFetch) throws PersistitException {

        Buffer buffer = null;

        try {
            byte[] rawBytes = value.getEncodedBytes();
            int rawSize = value.getEncodedSize();
            if (rawSize != LONGREC_SIZE) {
                corrupt("Invalid LONG_RECORD value size=" + rawSize + " but should be " + LONGREC_SIZE);
            }
            if ((rawBytes[0] & 0xFF) != LONGREC_TYPE) {
                corrupt("Invalid LONG_RECORD value type=" + (rawBytes[0] & 0xFF) + " but should be " + LONGREC_TYPE);
            }
            int longSize = Buffer.decodeLongRecordDescriptorSize(rawBytes, 0);
            long startAtPage = Buffer.decodeLongRecordDescriptorPointer(rawBytes, 0);

            int remainingSize = Math.min(longSize, minimumBytesToFetch);

            value.ensureFit(remainingSize);
            value.setEncodedSize(remainingSize);

            int offset = 0;
            System.arraycopy(rawBytes, LONGREC_PREFIX_OFFSET, value.getEncodedBytes(), offset, LONGREC_PREFIX_SIZE);

            offset += LONGREC_PREFIX_SIZE;
            remainingSize -= LONGREC_PREFIX_SIZE;
            long page = startAtPage;

            for (int count = 0; page != 0 && offset < minimumBytesToFetch; count++) {
                if (remainingSize <= 0) {
                    corrupt("Invalid LONG_RECORD remaining size=" + remainingSize + " of " + rawSize + " in page "
                            + page);
                }
                buffer = _volume.getPool().get(_volume, page, false, true);
                if (buffer.getPageType() != PAGE_TYPE_LONG_RECORD) {
                    corrupt("LONG_RECORD chain is invalid at page " + page + " - invalid page type: " + buffer);
                }
                int segmentSize = buffer.getBufferSize() - HEADER_SIZE;
                if (segmentSize > remainingSize) {
                    segmentSize = remainingSize;
                }

                System.arraycopy(buffer.getBytes(), HEADER_SIZE, value.getEncodedBytes(), offset, segmentSize);

                offset += segmentSize;
                remainingSize -= segmentSize;
                // previousPage = page;
                page = buffer.getRightSibling();
                buffer.releaseTouched();
                buffer = null;

                if (count > MAX_LONG_RECORD_CHAIN) {
                    if (count > MAX_LONG_RECORD_CHAIN) {
                        corrupt("LONG_RECORD chain starting at " + startAtPage + " is too long");
                    }

                }
            }
            value.setLongSize(rawSize);
            value.setEncodedSize(offset);
        } finally {
            if (buffer != null) {
                buffer.releaseTouched();
            }
        }
    }

    /**
     * Create a new LONG_RECORD chain and stores the supplied byte array in the
     * pages of this chain. The chain is written in right-to-left order so that
     * any page having a right pointer points to a valid successor.
     * 
     * Each page is written with its own timestamp (necessary to satisfy write
     * order invariant). Therefore a checkpoint could occur during the middle,
     * after some pages have been assigned a timestamp and before others. This
     * means that a crash recovery could recover the tail of a chain, but not
     * its head. This does not cause corruption, but does cause permanent loss
     * of the pages that were recovered at the right end but never linked to a
     * data page. Current remedy: save/reload data. Such dangling chains can be
     * detected by IntegrityCheck and a future remedy would be for
     * IntegrityCheck to move them back to the garbage chain.
     * 
     * If this method is called in the context of a transaction, it writes each
     * page immediately to the journal. This allows recovery to rebuild the long
     * record for a recovered transaction that committed after the keystone
     * checkpoint.
     * 
     * @param value
     *            The value. Must be in "long record mode"
     * @param inTxn
     *            indicates whether this operation is within the context of a
     *            transaction.
     * 
     * @throws PersistitException
     */
    long storeLongRecord(final Value value, final boolean inTxn) throws PersistitException {
        value.changeLongRecordMode(true);

        // Calculate how many LONG_RECORD pages we will need.
        //
        boolean completed = false;
        int longSize = value.getLongSize();
        byte[] longBytes = value.getLongBytes();
        byte[] rawBytes = value.getEncodedBytes();
        int maxSegmentSize = _volume.getPool().getBufferSize() - HEADER_SIZE;

        Debug.$assert0.t(value.isLongRecordMode());
        Debug.$assert0.t(rawBytes.length == LONGREC_SIZE);

        System.arraycopy(longBytes, 0, rawBytes, LONGREC_PREFIX_OFFSET, LONGREC_PREFIX_SIZE);

        long looseChain = 0;

        sequence(LONG_RECORD_ALLOCATE_A);

        Buffer buffer = null;
        int offset = LONGREC_PREFIX_SIZE + (((longSize - LONGREC_PREFIX_SIZE - 1) / maxSegmentSize) * maxSegmentSize);
        try {
            for (;;) {
                while (offset >= LONGREC_PREFIX_SIZE) {
                    buffer = _volume.getStructure().allocPage();
                    final long timestamp = _persistit.getTimestampAllocator().updateTimestamp();
                    buffer.writePageOnCheckpoint(timestamp);
                    buffer.init(PAGE_TYPE_LONG_RECORD);

                    int segmentSize = longSize - offset;
                    if (segmentSize > maxSegmentSize)
                        segmentSize = maxSegmentSize;

                    Debug.$assert0.t(segmentSize >= 0 && offset >= 0 && offset + segmentSize <= longBytes.length
                            && HEADER_SIZE + segmentSize <= buffer.getBytes().length);

                    System.arraycopy(longBytes, offset, buffer.getBytes(), HEADER_SIZE, segmentSize);

                    int end = HEADER_SIZE + segmentSize;
                    if (end < buffer.getBufferSize()) {
                        buffer.clearBytes(end, buffer.getBufferSize());
                    }
                    buffer.setRightSibling(looseChain);
                    looseChain = buffer.getPageAddress();
                    buffer.setDirtyAtTimestamp(timestamp);
                    if (inTxn) {
                        buffer.writePage();
                    }
                    buffer.releaseTouched();
                    offset -= maxSegmentSize;
                    buffer = null;
                }

                long page = looseChain;
                looseChain = 0;
                Buffer.writeLongRecordDescriptor(value.getEncodedBytes(), longSize, page);
                completed = true;

                return page;
            }
        } finally {
            if (buffer != null)
                buffer.releaseTouched();
            if (looseChain != 0) {
                _volume.getStructure().deallocateGarbageChain(looseChain, 0);
            }
            if (!completed) {
                value.changeLongRecordMode(false);
            }
        }
    }

    void corrupt(final String error) throws CorruptVolumeException {
        Debug.$assert0.t(false);
        if (_exchange != null) {
            _persistit.getLogBase().corruptVolume.log(error + Util.NEW_LINE + _exchange.toStringDetail());
        } else {
            _persistit.getLogBase().corruptVolume.log(error);
        }
        throw new CorruptVolumeException(error);
    }

}
