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
     * pages of this chain.
     * 
     * @param value
     *            The value. Must be in "long record mode"
     * 
     * @param from
     *            Offset to first byte of the long record.
     * 
     * @return Page address of the beginning of the chain
     * 
     * @throws PersistitException
     */
    long storeLongRecord(final Value value, final long timestamp, final boolean inTxn) throws PersistitException {
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
