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
    private final static int UNDEFINED_VALUE_LENGTH = 0;
    
    private final static int LENGTH_TYPE_MVV = 1;       // byte
    private final static int LENGTH_VERSION_HANDLE = 8; // long
    private final static int LENGTH_VALUE_LENGTH = 2;   // short
    private final static int LENGTH_PER_VERSION = LENGTH_VERSION_HANDLE + LENGTH_VALUE_LENGTH;


    static int overheadLength(int numVersions) {
        if(numVersions > 1) {
            return LENGTH_TYPE_MVV + LENGTH_PER_VERSION*numVersions;
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

    static void assertCapacity(byte[] dest, int length) throws IllegalArgumentException {
        if(dest.length < length) {
            throw new IllegalArgumentException("Destination array not big enough: " + dest.length + " < " + length);
        }
    }


    static int storeValue(byte[] source, int sourceLength, long versionHandle, byte[] dest, int destLength) {
        int offset = 0;
        if(destLength == 0) {
            assertCapacity(dest, overheadLength(2) + sourceLength);
            
            // Promote to MVV, original state is undefined
            dest[offset++] = TYPE_MVV;
            offset += writeVersionHandle(dest, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(dest, offset, UNDEFINED_VALUE_LENGTH);
        }
        else if(dest[0] != TYPE_MVV) {
            assertCapacity(dest, overheadLength(2) + destLength + sourceLength);

            // Promote to MVV, shift existing down for header
            System.arraycopy(dest, 0, dest, LENGTH_TYPE_MVV + LENGTH_PER_VERSION, destLength);
            dest[offset++] = TYPE_MVV;
            offset += writeVersionHandle(dest, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(dest, offset, destLength);
            offset += destLength;
        }
        else {
            // Search for versionHandle
            //   if size == sourceLength, plop down over
            //   else shift remaining left, add to end
            int curOffset = 1;
            while(curOffset < destLength) {
                final long version = Util.getLong(dest, curOffset);
                final int size = Util.getShort(dest, curOffset + LENGTH_VERSION_HANDLE);
                final int chunkOffset = LENGTH_PER_VERSION + size;
                curOffset += chunkOffset;
                if(version == versionHandle) {
                    if(size == sourceLength) {
                        System.arraycopy(source, 0, dest, curOffset - size, sourceLength);
                        return destLength;
                    }
                    else {
                        assertCapacity(dest, destLength - size + sourceLength);
                        
                        System.arraycopy(dest, curOffset, dest, curOffset - chunkOffset, destLength - curOffset);
                        destLength -= chunkOffset;
                        break;
                    }
                }
            }

            assertCapacity(dest, destLength + LENGTH_PER_VERSION + sourceLength);
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
