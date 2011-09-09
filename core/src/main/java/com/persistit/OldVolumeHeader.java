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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

/**
 * A volume header represents the header information at the beginning of each
 * volume file.
 */
public class OldVolumeHeader {

    /**
     * Signature value - human and machine readable confirmation that this file
     * resulted from Persistit.
     */
    final static byte[] SIGNATURE = Util.stringToBytes("PERSISTIT VOLUME");

    /**
     * Current product version number.
     */
    final static int VERSION = 221;

    /**
     * Minimum product version that can handle Volumes created by this version.
     */
    final static int MIN_SUPPORTED_VERSION = 210;

    /**
     * Minimum product version that can handle Volumes created by this version.
     */
    final static int MAX_SUPPORTED_VERSION = 299;

    final static int SIZE = Buffer.MIN_BUFFER_SIZE;

    private FileChannel channel;

    public OldVolumeHeader(FileChannel channel) {
        this.channel = channel;
    }

    /**
     * Validate that the header conforms to the volume header specification.
     * CorruptVolumeExceptions are thrown when an inconsistency is observed.
     * 
     * @return ByteBuffer populated with volume header information
     * @throws PersistitException
     * @throws IOException
     */
    public ByteBuffer validate() throws CorruptVolumeException, IOException {
        assert channel != null;
        if (channel.size() < SIZE) {
            throw new CorruptVolumeException("Volume file too short: " + channel.size());
        }

        final ByteBuffer bb = ByteBuffer.allocate(SIZE);
        final byte[] bytes = bb.array();
        channel.read(bb, 0);

        /*
         * Check out the fixed Volume file and learn the buffer size.
         */
        if (!Util.bytesEqual(bytes, 0, SIGNATURE)) {
            throw new CorruptVolumeException("Invalid signature");
        }

        int version = Util.getInt(bytes, 16);
        if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) {
            throw new CorruptVolumeException("Unsupported version " + version + " (must be in range "
                    + MIN_SUPPORTED_VERSION + " - " + MAX_SUPPORTED_VERSION + ")");
        }

        return bb;
    }

    /**
     * @return signature for this volume
     */
    public byte[] getSignature() {
        return SIGNATURE;
    }

    /**
     * @return version of persistit for this header
     */
    public int getVersion() {
        return VERSION;
    }

    /**
     * @return size of this header
     */
    public int getSize() {
        return SIZE;
    }

}
