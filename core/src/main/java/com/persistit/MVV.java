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
    public final static int VERSION_NOT_FOUND = -1;

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


    static int storeVersion(byte[] source, int sourceLength, long versionHandle, byte[] dest, int destLength) {
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

    public static interface FetchSpecifier {
        void init();
        void sawVersion(long versionHandle, int valueLength, int offset);
        int offsetToFetch();
    }

    public static class ExactVersionSpecifier implements FetchSpecifier {
        private int offset;
        private long desiredVersion;

        public ExactVersionSpecifier(long desiredVersion) {
            this.desiredVersion = desiredVersion;
        }
        
        @Override
        public void init() {
            offset = VERSION_NOT_FOUND;
        }

        @Override
        public void sawVersion(long versionHandle, int valueLength, int offset) {
            if(versionHandle == desiredVersion) {
                this.offset = offset;
            }
        }

        @Override
        public int offsetToFetch() {
            return offset;
        }
    }

    static int fetchVersionBySpecifier(FetchSpecifier specifier, byte[] source, int sourceLength, byte[] dest) {
        specifier.init();
        if(sourceLength == 0) {
            specifier.sawVersion(PRIMORDIAL_VALUE_VERSION, UNDEFINED_VALUE_LENGTH, 0);
        }
        else if(source[0] != TYPE_MVV) {
            specifier.sawVersion(PRIMORDIAL_VALUE_VERSION, sourceLength, 0);
        }
        else {
            int offset = 1;
            while(offset < sourceLength) {
                final long version = Util.getLong(source, offset);
                final int valueLength = Util.getShort(source, offset + LENGTH_VERSION_HANDLE);
                offset += LENGTH_PER_VERSION;
                specifier.sawVersion(version, valueLength, offset);
                offset += valueLength;
            }
        }

        int returnSize = VERSION_NOT_FOUND;
        int offsetToReturn = specifier.offsetToFetch();
        if(offsetToReturn > 0) {
            if(offsetToReturn == 0 && sourceLength == 0) {
                returnSize = UNDEFINED_VALUE_LENGTH;
            }
            else {
                returnSize = Util.getShort(source, offsetToReturn - LENGTH_VALUE_LENGTH);
                assertCapacity(dest, returnSize);
                System.arraycopy(source, offsetToReturn, dest, 0, returnSize);
            }
        }

        return returnSize;
    }
}
