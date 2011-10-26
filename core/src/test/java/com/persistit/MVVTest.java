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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MVVTest {
    private static final int MVI = 254;
    private static final int TEST_ARRAY_LENGTH = 50;

    private byte[] dest;
    private byte[] source;


    @Before
    public final void setUp() {
        dest = new byte[TEST_ARRAY_LENGTH];
        source = new byte[TEST_ARRAY_LENGTH];
    }

    @After
    public final void tearDown() {
        dest = null;
        source = null;
    }

    private static int writeArray(byte[] array, int... contents) {
        assert contents.length <= TEST_ARRAY_LENGTH : "Too many values for test array";
        for(int i = 0; i < contents.length; ++i) {
            int value = contents[i];
            assert value >= 0 && value <= 255 : "Value " + value + " out of byte range at index " + i;
            array[i] = (byte)value;
        }
        return contents.length;
    }

    private static byte[] newArray(int... contents) {
        byte[] array = new byte[contents.length];
        writeArray(array, contents);
        return array;
    }

    private static void assertArrayEqualsLen(byte[] expected, byte[] actual, int length) {
        if(expected.length < length) {
            throw new AssertionError(String.format("Expected array is too short: %d vs %d", actual.length, length));
        }
        if(actual.length < length) {
            throw new AssertionError(String.format("Actual array is too short: %d vs %d", actual.length, length));
        }
        for(int i = 0; i < length; ++i) {
            byte bE = expected[i];
            byte bA = actual[i];
            if(bE != bA) {
                throw new AssertionError(String.format("Arrays differed at element [%d]: expected <%d> but was <%d>", i, bE, bA));
            }
        }
    }

    @Test
    public void storeToUndefined() {
        final int vh = 200;
        final int sourceLength = writeArray(source, 0xA,0xB,0xC);
        final int storedLength = MVV.storeValue(source, sourceLength, vh, dest, 0);

        assertEquals(sourceLength + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,0,  0,0,
                                      0,0,0,0,0,0,0,vh, 0,3, 0xA,0xB,0xC),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToPrimordial() {
        final int vh = 200;
        final int destLength = writeArray(dest, 0xA,0xB,0xC);
        final int sourceLength = writeArray(source, 0xD,0xE,0xF);
        final int storedLength = MVV.storeValue(source, sourceLength, vh, dest, destLength);
        
        assertEquals(destLength + sourceLength + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,0,  0,3, 0xA,0xB,0xC,
                                      0,0,0,0,0,0,0,vh, 0,3, 0xD,0xE,0xF),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToExisting() {
        final int vh1 = 10, vh2 = 200;
        final int destContentsLength = 4;
        final int destLength = writeArray(dest,
                                          MVI,
                                          0,0,0,0,0,0,0,vh1, 0,4, 0xA,0xB,0xC,0xD);
        final int sourceLength = writeArray(source, 0xE,0xF);
        final int storedLength = MVV.storeValue(source, sourceLength, vh2, dest, destLength);

        assertEquals(destContentsLength + sourceLength + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,vh1, 0,4, 0xA,0xB,0xC,0xD,
                                      0,0,0,0,0,0,0,vh2, 0,2, 0xE,0xF),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToExistingVersionEqualLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final int destLength = writeArray(dest,
                                          MVI,
                                          0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                          0,0,0,0,0,0,0,vh2, 0,3, 0xA,0xB,0xC,
                                          0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9);
        final int sourceLength = writeArray(source, 0xD,0xE,0xF);
        final int storedLength = MVV.storeValue(source, sourceLength, vh2, dest, destLength);

        assertEquals(destLength, storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                      0,0,0,0,0,0,0,vh2, 0,3, 0xD,0xE,0xF,
                                      0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToExistingVersionShorterLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final int destLength = writeArray(dest,
                                          MVI,
                                          0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                          0,0,0,0,0,0,0,vh2, 0,3, 0xA,0xB,0xC,
                                          0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9);
        final int sourceLength = writeArray(source, 0xD,0xE);
        final int storedLength = MVV.storeValue(source, sourceLength, vh2, dest, destLength);

        assertEquals(destLength - 1, storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                     0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                     0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9,
                                     0,0,0,0,0,0,0,vh2, 0,2, 0xD,0xE),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToExistingVersionLongerLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final int destLength = writeArray(dest,
                                          MVI,
                                          0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                          0,0,0,0,0,0,0,vh2, 0,3, 0xA,0xB,0xC,
                                          0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9);
        final int sourceLength = writeArray(source, 0xC,0xD,0xE,0xF);
        final int storedLength = MVV.storeValue(source, sourceLength, vh2, dest, destLength);

        assertEquals(destLength + 1, storedLength);
        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,vh1, 0,2, 0x4,0x5,
                                      0,0,0,0,0,0,0,vh3, 0,4, 0x6,0x7,0x8,0x9,
                                      0,0,0,0,0,0,0,vh2, 0,4, 0xC,0xD,0xE,0xF),
                             dest,
                             storedLength);
    }

    @Test
    public void storeToExistingVersionIfComparedAsInt() {
        final long vh1 = 0x0000000000AABBCCL;
        final long vh2 = 0x00FFFFFF00AABBCCL;

        int destLength = 0;

        writeArray(source, 0xA);
        destLength = MVV.storeValue(source, 1, vh1, dest, destLength);

        writeArray(source, 0xB);
        destLength = MVV.storeValue(source, 1, vh2, dest, destLength);

        assertArrayEqualsLen(newArray(MVI,
                                      0,0,0,0,0,0,0,0,                   0,0,
                                      0,0,0,0,0,0xAA,0xBB,0xCC,          0,1, 0xA,
                                      0,0xFF,0xFF,0xFF,0,0xAA,0xBB,0xCC, 0,1, 0xB),
                             dest,
                             destLength);
    }

    @Test
    public void storeBigVersions() {
        final long versions[] = {
                Integer.MAX_VALUE,
                10,
                1844674407370955161L /* MAX/5-ish */,
                Short.MAX_VALUE,
                Long.MAX_VALUE,
                8301034833169298227L /* MAX*0.9-ish */
        };
        final byte contents[][] = {
                newArray(0xA0),
                newArray(0xB0,0xB1),
                newArray(0xC0,0xC1,0xC2),
                newArray(0xD0,0xD1,0xD2,0xD3),
                newArray(0xE0,0xE1,0xE2,0xE3,0xE4),
                newArray(0xF0,0xF1,0xF2,0xF3,0xF4,0xF5)
        };

        assertEquals(versions.length, contents.length);

        // Build expected
        final byte[] expectedArray = new byte[1000];
        expectedArray[0] = (byte)MVI;
        Util.putLong(expectedArray, 1, 0);
        Util.putShort(expectedArray, 9, 0);
        int off = 11;

        for(int i = 0; i < versions.length; ++i) {
            Util.putLong(expectedArray, off, versions[i]);
            Util.putShort(expectedArray, off+8, contents[i].length);
            System.arraycopy(contents[i], 0, expectedArray, off+10, contents[i].length);
            off += 10 + contents[i].length;
        }
        
        // Build actual
        int destLength = 0;
        dest = new byte[1000];
        for(int i = 0; i < versions.length; ++i) {
            destLength = MVV.storeValue(contents[i], contents[i].length, versions[i], dest, destLength);
        }

        assertArrayEqualsLen(expectedArray, dest, destLength);
    }


    //
    // Basic fetch functionality
    //

    @Test(expected=UnsupportedOperationException.class)
    public void fetchFromUndefinedBasic() {
        final int vh = 200;
        final int sourceLen = writeArray(source, 0xA,0xB,0xC);
        MVV.fetchValue(source, sourceLen, vh, dest, 0);
    }
}
