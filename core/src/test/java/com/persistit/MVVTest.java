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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MVVTest {
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
            assert value >= 0 && value <= 255 : "Value out of byte range at index " + i;
            array[i] = (byte)value;
        }
        return contents.length;
    }

    private static byte[] newArray(int... contents) {
        byte[] array = new byte[TEST_ARRAY_LENGTH];
        writeArray(array, contents);
        return array;
    }

    
    @Test
    public void storeToUndefinedBasic() {
        final int sourceLength = writeArray(source, 0xA,0xB,0xC);
        final long version = 200;
        int finalLength = MVV.storeValue(source, sourceLength, version, dest, 0);

        assertEquals(sourceLength + MVV.overheadLength(2), finalLength);
        assertArrayEquals(newArray(254, 0,0,0,0,0,0,0,0, 0,0, 0,0,0,0,0,0,0,200, 0,3, 0xA,0xB,0xC), dest);
    }

    @Test
    public void storeToPrimordialBasic() {
        final int destLength = writeArray(dest, 0xA,0xB,0xC);
        final int sourceLength = writeArray(source, 0xD,0xE,0xF);
        final long version = 200;
        int finalLength = MVV.storeValue(source, sourceLength, version, dest, destLength);
        
        assertEquals(destLength + sourceLength + MVV.overheadLength(2), finalLength);
        assertArrayEquals(newArray(254, 0,0,0,0,0,0,0,0, 0,3, 0xA,0xB,0xC, 0,0,0,0,0,0,0,200, 0,3, 0xD,0xE,0xF), dest);
    }

    @Test
    public void storeToExistingBasic() {
        final int destContentsLength = 4;
        final int destLength = writeArray(dest, 254, 0,0,0,0,0,0,0,10, 0,4, 0xA,0xB,0xC,0xD);
        final int sourceLength = writeArray(source, 0xE,0xF);
        final long version = 200;
        int finalLength = MVV.storeValue(source, sourceLength, version, dest, destLength);

        assertEquals(destContentsLength + sourceLength + MVV.overheadLength(2), finalLength);
        assertArrayEquals(newArray(254, 0,0,0,0,0,0,0,10, 0,4, 0xA,0xB,0xC,0xD, 0,0,0,0,0,0,0,200, 0,2, 0xE,0xF), dest);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void fetchFromUndefinedBasic() {
        final int sourceLen = writeArray(source, 0xA,0xB,0xC);
        final int version = 200;
        MVV.fetchValue(source, sourceLen, version, dest, 0);
    }
}
