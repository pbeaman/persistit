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

    /**
     * Get the length of the overhead per version stored in an MVV array.
     * @return length of overhead for any single version
     */
    static int perVersionHeaderLength() {
        return LENGTH_PER_VERSION;
    }

    /**
     * Get the full length of all overhead for the desired number of versions. Note that a single
     * version has no overhead.
     * @param numVersions Number of versions to compute overhead for.
     * @return length of all overhead
     */
    static int overheadLength(int numVersions) {
        if(numVersions > 1) {
            return LENGTH_TYPE_MVV + LENGTH_PER_VERSION*numVersions;
        }
        return 0;
    }

    /**
     * Compute and estimate of the required length to append a new version to the given MVV array. This
     * method is a loose estimate and may be greater, but never less, than the length ultimately needed.
     * @param source MVV array of any size/state
     * @param newVersionLength Length of the new version that will be put into {@code source}
     * @return Required length estimate
     */
    static int estimateRequiredLength(byte[] source, int newVersionLength) {
        if(source.length == 0 || source[0] != TYPE_MVV) {
            return overheadLength(2) + source.length + newVersionLength;
        }
        else {
            return source.length + LENGTH_PER_VERSION + newVersionLength;
        }
    }

    /**
     * Compute the final length needed to add, or update, a specific version into the given MVV array.
     * More costly than {@link #estimateRequiredLength(byte[], int)} as all existing versions must be
     * inspected to handle update cases.
     * @param source MVV array of any size/state
     * @param versionHandle Version to be inserted
     * @param newVersionLength Length of version being inserted
     * @return Exact required length
     */
    static int exactRequiredLength(byte[] source, long versionHandle, int newVersionLength) {
        if(source.length == 0 || source[0] != TYPE_MVV) {
            return overheadLength(2) + source.length + newVersionLength;
        }
        else {
            int offset = 1;
            while(offset < source.length) {
                final long version = Util.getLong(source, offset);
                final int valueLength = Util.getShort(source, offset + LENGTH_VERSION_HANDLE);
                offset += LENGTH_PER_VERSION + valueLength;
                if(version == versionHandle) {
                    return source.length - valueLength + newVersionLength;
                }
            }
            return source.length + LENGTH_PER_VERSION + newVersionLength;
        }
    }

    /**
     * Write a value, with the specified version and length, into the given MVV byte array.
     * The resulting contents of the destination array will always be MVV no matter the starting
     * state (ie. empty, primordial, or MVV). That is, an empty or primordial dest will be promoted
     * to an MVV and an MVV will get appended to. If the exact version already exists in the
     * destination array if will be updated as required.
     * @param source Value to add to dest
     * @param sourceLength Length of source currently used
     * @param versionHandle Version associated with source
     * @param dest Existing and target MVV array
     * @param destLength Length of dest currently used
     * @return New consumed length of dest
     * @throws IllegalArgumentException If dest is too small to hold final MVV contents
     */
    public static int storeVersion(byte[] source, int sourceLength, long versionHandle, byte[] dest, int destLength) {
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

    /**
     * Interface for fetching versions of values out of an MVV array.
     */
    public static interface FetchVisitor {
        /**
         * Called before iterating over MVV array.
         */
        void init();

        /**
         * Called exactly once for each version in the MVV array.
         * @param versionHandle Version
         * @param valueLength Length of stored value
         * @param offset Offset in MVV byte array
         */
        void sawVersion(long versionHandle, int valueLength, int offset);

        /**
         * After iteration is complete, this method is called to determine what
         * version, by way of the offset, to return.
         * @return Offset of the desired version or {@link MVV#VERSION_NOT_FOUND} for none.
         */
        int offsetToFetch();
    }

    /**
     * Simple fetch visitor that looks for an exact version.
     */
    public static class ExactVersionVisitor implements FetchVisitor {
        private int offset;
        private long desiredVersion;

        public ExactVersionVisitor(long desiredVersion) {
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

    /**
     * Copy a version of a value, specified by the given visitor, into the destination byte array.
     * @param visitor Visitor to specify what version to return
     * @param source MVV array to search
     * @param sourceLength Valid length of source
     * @param dest Array to copy desired value to
     * @return Length of new contents in dest or {@link MVV#VERSION_NOT_FOUND} if no value was copied.
     * @throws IllegalArgumentException If the destination is too small to hold the desired value
     */
    public static int fetchVersionByVisitor(FetchVisitor visitor, byte[] source, int sourceLength, byte[] dest) {
        visitor.init();
        if(sourceLength == 0) {
            visitor.sawVersion(PRIMORDIAL_VALUE_VERSION, UNDEFINED_VALUE_LENGTH, 0);
        }
        else if(source[0] != TYPE_MVV) {
            visitor.sawVersion(PRIMORDIAL_VALUE_VERSION, sourceLength, 0);
        }
        else {
            int offset = 1;
            while(offset < sourceLength) {
                final long version = Util.getLong(source, offset);
                final int valueLength = Util.getShort(source, offset + LENGTH_VERSION_HANDLE);
                offset += LENGTH_PER_VERSION;
                visitor.sawVersion(version, valueLength, offset);
                offset += valueLength;
            }
        }

        int returnSize = VERSION_NOT_FOUND;
        int offsetToReturn = visitor.offsetToFetch();
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
    

    /**
     * Internal helper. Write a version handle into the given byte array at the specified offset.
     * @param dest Destination array
     * @param offset Position in dest to write
     * @param versionHandle Version to write
     * @return Byte count consumed in dest
     */
    private static int writeVersionHandle(byte[] dest, int offset, long versionHandle) {
        Util.putLong(dest, offset, versionHandle);
        return LENGTH_VERSION_HANDLE;
    }

    /**
     * Internal helper. Write a value length given byte array at the specified offset.
     * @param dest Destination array
     * @param offset Position in dest to write
     * @param length Length to write
     * @return Byte count consumed in dest
     */
    private static int writeValueLength(byte[] dest, int offset, int length) {
        Util.putShort(dest, offset, length);
        return LENGTH_VALUE_LENGTH;
    }

    /**
     * Internal helper. Used to assert a given byte array is large enough before writing.
     * @param dest Array to check
     * @param length Length needed
     * @throws IllegalArgumentException If the array is too small
     */
    private static void assertCapacity(byte[] dest, int length) throws IllegalArgumentException {
        if(dest.length < length) {
            throw new IllegalArgumentException("Destination array not big enough: " + dest.length + " < " + length);
        }
    }
}
