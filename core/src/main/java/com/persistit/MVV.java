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
    final static int TYPE_MVV = 0xFE;

    private static final byte TYPE_MVV_BYTE = (byte)TYPE_MVV;
    private final static int PRIMORDIAL_VALUE_VERSION = 0;
    private final static int UNDEFINED_VALUE_LENGTH = 0;
    public final static int VERSION_NOT_FOUND = -1;

    private final static int LENGTH_TYPE_MVV = 1;       // byte
    private final static int LENGTH_VERSION = 8;        // long
    private final static int LENGTH_VALUE_LENGTH = 2;   // short

    private final static int LENGTH_PER_VERSION = LENGTH_VERSION + LENGTH_VALUE_LENGTH;


    /**
     * Get the full length of all overhead for the desired number of versions. Note in practice
     * a single version, i.e. primordial, doesn't actually have any overhead.
     *
     * @param numVersions Number of versions to compute overhead for.
     * @return length of all overhead
     */
    static int overheadLength(int numVersions) {
        return LENGTH_TYPE_MVV + LENGTH_PER_VERSION * numVersions;
    }

    /**
     * Compute and estimate of the required length to append a new version to the given MVV array. This
     * method is a loose estimate and may be greater, but never less, than the length ultimately needed.
     * 
     * @param source MVV array of any size/state
     * @param sourceLength Length of {@code source} currently in use
     * @param newVersionLength Length of the new version that will be put into {@code source}
     * @return Required length estimate
     */
    static int estimateRequiredLength(byte[] source, int sourceLength, int newVersionLength) {
        if(sourceLength == 0 || source[0] != TYPE_MVV_BYTE) {
            return overheadLength(2) + sourceLength + newVersionLength;
        }
        else {
            return sourceLength + LENGTH_PER_VERSION + newVersionLength;
        }
    }

    /**
     * Compute the final length needed to add, or update, a specific version into the given MVV array.
     * More costly than {@link #estimateRequiredLength(byte[], int, int)} as all existing versions must be
     * inspected to handle update cases.
     * 
     * @param source MVV array of any size/state
     * @param sourceLength Length of {@code source} currently in use
     * @param newVersion Version to be inserted
     * @param newVersionLength Length of version being inserted
     * @return Exact required length
     */
    static int exactRequiredLength(byte[] source, int sourceLength, long newVersion, int newVersionLength) {
        if(sourceLength == 0 || source[0] != TYPE_MVV_BYTE) {
            return overheadLength(2) + sourceLength + newVersionLength;
        }
        else {
            int offset = 1;
            while(offset < sourceLength) {
                final long version = Util.getLong(source, offset);
                final int valueLength = Util.getShort(source, offset + LENGTH_VERSION);
                offset += LENGTH_PER_VERSION + valueLength;
                if(version == newVersion) {
                    return sourceLength - valueLength + newVersionLength;
                }
            }
            return sourceLength + LENGTH_PER_VERSION + newVersionLength;
        }
    }

    /**
     * Write a value, with the specified version and length, into the given MVV byte array.
     * The resulting contents of the target array will always be MVV no matter the starting
     * state (ie. empty, primordial, or MVV). That is, an empty or primordial target will be
     * promoted to an MVV and an MVV will get appended to. If the exact version already exists
     * in the target array it will be updated as required.
     *
     * @param target Destination MVV array to append to or update
     * @param targetLength Length of target currently in use
     * @param version Version associated with source
     * @param source Value to store
     * @param sourceLength Length of source currently in use
     * @return New consumed length of target array
     * @throws IllegalArgumentException If target is too small to hold final MVV contents
     */
    public static int storeVersion(byte[] target, int targetLength, long version, byte[] source, int sourceLength) {
        int offset = 0;
        if(targetLength == 0) {
            assertCapacity(target, overheadLength(2) + sourceLength);
            
            // Promote to MVV, original state is undefined
            target[offset++] = TYPE_MVV_BYTE;
            offset += writeVersionHandle(target, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(target, offset, UNDEFINED_VALUE_LENGTH);
        }
        else if(target[0] != TYPE_MVV_BYTE) {
            assertCapacity(target, overheadLength(2) + targetLength + sourceLength);

            // Promote to MVV, shift existing down for header
            System.arraycopy(target, 0, target, LENGTH_TYPE_MVV + LENGTH_PER_VERSION, targetLength);
            target[offset++] = TYPE_MVV_BYTE;
            offset += writeVersionHandle(target, offset, PRIMORDIAL_VALUE_VERSION);
            offset += writeValueLength(target, offset, targetLength);
            offset += targetLength;
        }
        else {
            // Search for version
            //   if size == sourceLength, plop down over
            //   else shift remaining left, add to end
            int curOffset = 1;
            while(curOffset < targetLength) {
                final long curVersion = Util.getLong(target, curOffset);
                final int size = Util.getShort(target, curOffset + LENGTH_VERSION);
                final int chunkOffset = LENGTH_PER_VERSION + size;
                curOffset += chunkOffset;
                if(curVersion == version) {
                    if(size == sourceLength) {
                        System.arraycopy(source, 0, target, curOffset - size, sourceLength);
                        return targetLength;
                    }
                    else {
                        assertCapacity(target, targetLength - size + sourceLength);
                        
                        System.arraycopy(target, curOffset, target, curOffset - chunkOffset, targetLength - curOffset);
                        targetLength -= chunkOffset;
                        break;
                    }
                }
            }

            assertCapacity(target, targetLength + LENGTH_PER_VERSION + sourceLength);
            offset = targetLength;
        }

        // Append new value
        offset += writeVersionHandle(target, offset, version);
        offset += writeValueLength(target, offset, sourceLength);
        System.arraycopy(source, 0, target, offset, sourceLength);

        return offset + sourceLength;
    }

    /**
     * Search for a known version within a MVV array. If the version is found within the array,
     * copy the contents out to the target and return the consumed length.
     * 
     * @param source MVV array to search
     * @param sourceLength Valid length of source
     * @param version Version to search for
     * @param target Array to copy desired value to
     * @return Length of new contents in target or {@link MVV#VERSION_NOT_FOUND} if no value was copied.
     * @throws IllegalArgumentException If the target array is too small to hold the value
     */
    public static int fetchVersion(byte[] source, int sourceLength, long version, byte[] target) {
        int offset = 0;
        int length = VERSION_NOT_FOUND;

        if(version == 0 && (sourceLength == 0 || source[0] != TYPE_MVV_BYTE)) {
            length = sourceLength;
        }
        else if(sourceLength > 0 && source[0] == TYPE_MVV_BYTE) {
            offset = 1;
            while(offset < sourceLength) {
                long curVersion = Util.getLong(source, offset);
                int curLength = Util.getShort(source, offset + LENGTH_VERSION);
                offset += LENGTH_PER_VERSION;
                if(curVersion == version) {
                    length = curLength;
                    break;
                }
                offset += curLength;
            }
        }

        if(length > 0) {
            assertCapacity(target, length);
            System.arraycopy(source, offset, target, 0, length);
        }

        return length;
    }

    public static interface VersionVisitor {
        /**
         * Called before iterating over MVV array. Allows for instance re-use.
         */
        void init();

        /**
         * Called once, and only once, for each version in the MVV array.
         * 
         * @param version Version of the stored value
         * @param valueLength Length of stored value
         * @param offset Offset in MVV array to start of stored value
         */
        void sawVersion(long version, int valueLength, int offset);
    }

    /**
     * Enumerate all versions of the values contained within the given MVV array.
     * 
     * @param visitor FetchVisitor for consuming version info
     * @param source MVV array to search
     * @param sourceLength Consumed length of source
     */
    public static void visitAllVersions(VersionVisitor visitor, byte[] source, int sourceLength) {
        visitor.init();
        if(sourceLength == 0) {
            visitor.sawVersion(PRIMORDIAL_VALUE_VERSION, UNDEFINED_VALUE_LENGTH, 0);
        }
        else if(source[0] != TYPE_MVV_BYTE) {
            visitor.sawVersion(PRIMORDIAL_VALUE_VERSION, sourceLength, 0);
        }
        else {
            int offset = 1;
            while(offset < sourceLength) {
                final long version = Util.getLong(source, offset);
                final int valueLength = Util.getShort(source, offset + LENGTH_VERSION);
                offset += LENGTH_PER_VERSION;
                visitor.sawVersion(version, valueLength, offset);
                offset += valueLength;
            }
        }
    }

    /**
     * Fetch a version of a value from a MVV array given a known offset. The offset should
     * be the starting position of of the actual value and not the MVV header bytes. Intended
     * to be used in connection with the {@link #visitAllVersions(com.persistit.MVV.VersionVisitor, byte[], int)}
     * method which gives this offset.
     * 
     * @param source MVV array to search
     * @param sourceLength Consumed length of source
     * @param offset Offset inside {@code source} to start of actual value
     * @param target Array to copy desired value to
     * @return Length of new contents in target or {@link MVV#VERSION_NOT_FOUND} if no value was copied.
     * @throws IllegalArgumentException If the target array is too small to hold the value
     */
    public static int fetchVersionByOffset(byte[] source, int sourceLength, int offset, byte[] target) {
        if(offset < 0 || (offset > 0 && offset > sourceLength - LENGTH_VALUE_LENGTH)) {
            throw new IllegalArgumentException("Offset out of range: " + offset);
        }
        final int length = (offset == 0) ? sourceLength : Util.getShort(source, offset - LENGTH_VALUE_LENGTH);
        if(length > 0) {
            assertCapacity(target, length);
            System.arraycopy(source, offset, target, 0, length);
        }
        return length;
    }


    /**
     * Internal helper. Write a version handle into the given byte array at the specified offset.
     * 
     * @param target Destination array
     * @param offset Position in target to write
     * @param versionHandle Version to write
     * @return Byte count consumed in target
     */
    private static int writeVersionHandle(byte[] target, int offset, long versionHandle) {
        Util.putLong(target, offset, versionHandle);
        return LENGTH_VERSION;
    }

    /**
     * Internal helper. Write a value length given byte array at the specified offset.
     * 
     * @param target Destination array
     * @param offset Position in target to write
     * @param length Length to write
     * @return Byte count consumed in target
     */
    private static int writeValueLength(byte[] target, int offset, int length) {
        Util.putShort(target, offset, length);
        return LENGTH_VALUE_LENGTH;
    }

    /**
     * Internal helper. Used to assert a given byte array is large enough before writing.
     * 
     * @param target Array to check
     * @param length Length needed
     * @throws IllegalArgumentException If the array is too small
     */
    private static void assertCapacity(byte[] target, int length) throws IllegalArgumentException {
        if(target.length < length) {
            throw new IllegalArgumentException("Destination array not big enough: " + target.length + " < " + length);
        }
    }
}
