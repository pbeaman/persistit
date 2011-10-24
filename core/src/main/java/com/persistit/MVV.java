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

import com.persistit.util.Util;

public class MVV {
    private final static byte TYPE_MVV = (byte)0xFE;
    private final static int PRIMORDIAL_VALUE_VERSION = 0;

    private final static int LENGTH_TYPE = 1;           // byte
    private final static int LENGTH_VERSION_HANDLE = 8; // long
    private final static int LENGTH_VALUE_LENGTH = 2;   // unsigned short
    private final static int LENGTH_MVV_HEADER = LENGTH_TYPE + LENGTH_VERSION_HANDLE + LENGTH_VALUE_LENGTH;


    static int overheadLength(int numVersions) {
        if(numVersions > 1) {
            return LENGTH_TYPE + (LENGTH_VERSION_HANDLE + LENGTH_VALUE_LENGTH)*numVersions;
        }
        return 0;
    }


    private static int writeVersionHandle(byte[] dest, int offset, long versionHandle) {
        Util.putLong(dest, offset, versionHandle);
        return LENGTH_VERSION_HANDLE;
    }

    private static int writeValueLength(byte[] dest, int offset, int length) {
        Util.putShort(dest, offset, length);
        return LENGTH_VALUE_LENGTH;
    }


    static int storeValue(byte[] source, int sourceLength, long versionHandle, byte[] dest, int destLength) {
        int offset = 0;
        
        if(destLength == 0) {
            // Promote to MVV, original state is undefined
            dest[offset++] = TYPE_MVV;
            offset += writeVersionHandle(dest, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(dest, offset, 0);
        }
        else if(dest[0] != TYPE_MVV) {
            // Promote to MVV, shift existing down for header
            System.arraycopy(dest, offset, dest, LENGTH_MVV_HEADER, destLength);
            dest[offset++] = TYPE_MVV;
            offset += writeVersionHandle(dest, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(dest, offset, destLength);
            offset += destLength;
        }
        else {
            // TODO: Search for pre-existing value with same versionHandle
            // Just append to existing MVV
            offset = destLength;
        }

        // Append new value
        offset += writeVersionHandle(dest, offset, versionHandle);
        offset += writeValueLength(dest, offset, sourceLength);
        System.arraycopy(source, 0, dest, offset, sourceLength);

        return offset + sourceLength;
    }


    static void fetchValue(byte[] source, int sourceLength, long startTimestamp, byte[] dest, int destLength) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
