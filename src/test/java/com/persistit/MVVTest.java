/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.MVV.STORE_EXISTED_MASK;
import static com.persistit.MVV.STORE_LENGTH_MASK;
import static com.persistit.MVV.TYPE_MVV;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

public class MVVTest {
    @Test
    public void requireBigEndian() {
        // Tests have many expected arrays explicitly typed out, sanity check
        // setting
        assertEquals(true, Persistit.BIG_ENDIAN);
    }

    @Test
    public void lengthEstimate() {
        byte[] source = {};
        assertTrue(MVV.estimateRequiredLength(source, -1, 5) >= 5);
        assertTrue(MVV.estimateRequiredLength(source, source.length, 5) >= 5);

        source = newArray(0xA, 0xB, 0xC, 0xD, 0xE, 0xF);
        assertTrue(MVV.estimateRequiredLength(source, 2, 74) >= (2 + 74));
        assertTrue(MVV.estimateRequiredLength(source, source.length, 74) >= (source.length + 74));

        source = newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0x1, 0x2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 1, 0xA);
        assertTrue(MVV.estimateRequiredLength(source, source.length, 1) >= (source.length + 1));
    }

    @Test
    public void lengthExactly() {
        byte[] source = {};
        assertEquals(MVV.exactRequiredLength(source, 0, -1, 1, 5), MVV.overheadLength(1) + 5);
        assertEquals(MVV.exactRequiredLength(source, 0, source.length, 1, 5), MVV.overheadLength(2) + 5);

        source = newArray(0xA, 0xB, 0xC, 0xD, 0xE, 0xF);
        assertEquals(MVV.exactRequiredLength(source, 0, 2, 1, 74), MVV.overheadLength(2) + 2 + 74);
        assertEquals(MVV.exactRequiredLength(source, 0, source.length, 1, 74), MVV.overheadLength(2) + source.length
                + 74);

        source = newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0x1, 0x2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0xA, /* extra */
        0, 0, 0, 0, 0);
        final int usedLength = source.length - 5;
        // new version (-1 = MVV type ID)
        assertEquals(MVV.exactRequiredLength(source, 0, usedLength, 3, 3), usedLength + MVV.overheadLength(1) + 3 - 1);
        // replace version, shorter
        assertEquals(MVV.exactRequiredLength(source, 0, usedLength, 1, 1), usedLength - 1);
        // replace version, longer
        assertEquals(MVV.exactRequiredLength(source, 0, usedLength, 1, 3), usedLength + 1);
    }

    @Test
    public void isMVVAllInputs() {
        final byte[] empty = {};
        assertEquals("empty and unused", false, MVV.isArrayMVV(empty, 0, -1));
        assertEquals("empty and undefined", false, MVV.isArrayMVV(empty, 0, 0));

        final byte[] primordial = newArray(0xA, 0xB, 0xC);
        assertEquals("primordial and unused", false, MVV.isArrayMVV(primordial, 0, -1));
        assertEquals("primordial and used", false, MVV.isArrayMVV(primordial, 0, primordial.length));

        final byte[] mvvEmptyVersion = newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0);
        assertEquals("mvv empty value", true, MVV.isArrayMVV(mvvEmptyVersion, 0, mvvEmptyVersion.length));
        assertEquals("mvv array but unused", false, MVV.isArrayMVV(mvvEmptyVersion, 0, -1));

        final byte[] mvvTwoVersions = newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0xA, 0xB, 0, 0, 0, 0, 0, 0, 0,
                9, 0, 1, 0xC);
        assertEquals("mvv two versions", true, MVV.isArrayMVV(mvvTwoVersions, 0, mvvTwoVersions.length));
    }

    @Test
    public void storeToUnused() {
        final int vh = 200;
        final byte[] source = { 0xA, 0xB, 0xC };

        final byte[] target = new byte[100];
        final int storedLength = storeVersion(target, -1, vh, source, source.length);

        assertEquals(source.length + MVV.overheadLength(1), storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh, 0, 3, 0xA, 0xB, 0xC), target, storedLength);
    }

    @Test
    public void storeToUndefined() {
        final int vh = 200;
        final byte[] source = { 0xA, 0xB, 0xC };

        final byte[] target = new byte[100];
        final int storedLength = storeVersion(target, 0, vh, source, source.length);

        assertEquals(source.length + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, vh, 0, 3, 0xA, 0xB,
                0xC), target, storedLength);
    }

    @Test
    public void storeToPrimordial() {
        final int vh = 200;
        final byte[] source = { 0xD, 0xE, 0xF };
        final byte[] target = new byte[100];

        final int targetLength = writeArray(target, 0xA, 0xB, 0xC);
        final int storedLength = storeVersion(target, targetLength, vh, source, source.length);

        assertEquals(targetLength + source.length + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, vh,
                0, 3, 0xD, 0xE, 0xF), target, storedLength);
    }

    @Test
    public void storeToExisting() {
        final int vh1 = 10, vh2 = 200;

        final byte[] target = new byte[100];
        final int targetContentsLength = 4;
        final int targetLength = writeArray(target, TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 4, 0xA, 0xB, 0xC, 0xD);
        final byte[] source = { 0xE, 0xF };
        final int storedLength = storeVersion(target, targetLength, vh2, source, source.length);

        assertEquals(targetContentsLength + source.length + MVV.overheadLength(2), storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 4, 0xA, 0xB, 0xC, 0xD, 0, 0, 0, 0, 0, 0,
                0, vh2, 0, 2, 0xE, 0xF), target, storedLength);
    }

    @Test
    public void storeToExistingVersionEqualLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final byte[] target = new byte[100];
        final int targetLength = writeArray(target, TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0,
                0, 0, vh2, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, vh3, 0, 4, 0x6, 0x7, 0x8, 0x9);
        final byte[] source = { 0xD, 0xE, 0xF };
        final int storedLength = storeVersion(target, targetLength, vh2, source, source.length);
        assertTrue("version existed", (storedLength & STORE_EXISTED_MASK) != 0);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0, 0, 0, vh2, 0,
                3, 0xD, 0xE, 0xF, 0, 0, 0, 0, 0, 0, 0, vh3, 0, 4, 0x6, 0x7, 0x8, 0x9), target, storedLength);
    }

    @Test
    public void storeToExistingVersionShorterLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final byte[] target = new byte[100];
        final int targetLength = writeArray(target, TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0,
                0, 0, vh2, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, vh3, 0, 4, 0x6, 0x7, 0x8, 0x9);
        final byte[] source = { 0xD, 0xE };
        int storedLength = storeVersion(target, targetLength, vh2, source, source.length);
        assertTrue("version existed", (storedLength & STORE_EXISTED_MASK) != 0);

        storedLength &= STORE_LENGTH_MASK;
        assertEquals(targetLength - 1, storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0, 0, 0, vh3, 0,
                4, 0x6, 0x7, 0x8, 0x9, 0, 0, 0, 0, 0, 0, 0, vh2, 0, 2, 0xD, 0xE), target, storedLength);
    }

    @Test
    public void storeToExistingVersionLongerLength() {
        final int vh1 = 199, vh2 = 200, vh3 = 201;
        final byte[] target = new byte[100];
        final int targetLength = writeArray(target, TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0,
                0, 0, vh2, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, vh3, 0, 4, 0x6, 0x7, 0x8, 0x9);
        final byte[] source = { 0xC, 0xD, 0xE, 0xF };
        int storedLength = storeVersion(target, targetLength, vh2, source, source.length);
        assertTrue("version existed", (storedLength & STORE_EXISTED_MASK) != 0);

        storedLength &= STORE_LENGTH_MASK;

        assertEquals(targetLength + 1, storedLength);
        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, vh1, 0, 2, 0x4, 0x5, 0, 0, 0, 0, 0, 0, 0, vh3, 0,
                4, 0x6, 0x7, 0x8, 0x9, 0, 0, 0, 0, 0, 0, 0, vh2, 0, 4, 0xC, 0xD, 0xE, 0xF), target, storedLength);
    }

    @Test
    public void storeToExistingVersionIfComparedAsInt() {
        final long vh1 = 0x0000000000AABBCCL;
        final byte[] source1 = { 0xA };
        final long vh2 = 0x00FFFFFF00AABBCCL;
        final byte[] source2 = { 0xB };

        int targetLength = 0;
        final byte[] target = new byte[100];
        targetLength = storeVersion(target, targetLength, vh1, source1, source1.length);
        targetLength = storeVersion(target, targetLength, vh2, source2, source2.length);

        assertArrayEqualsLen(newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0, 1,
                0xA, 0, 0xFF, 0xFF, 0xFF, 0, 0xAA, 0xBB, 0xCC, 0, 1, 0xB), target, targetLength);
    }

    @Test
    public void storeBigVersions() {
        final long versions[] = { Integer.MAX_VALUE, 10, 1844674407370955161L /*
                                                                               * MAX
                                                                               * /
                                                                               * 5
                                                                               * -
                                                                               * ish
                                                                               */, Short.MAX_VALUE, Long.MAX_VALUE,
                8301034833169298227L /* MAX*0.9-ish */
        };
        final byte contents[][] = { newArray(0xA0), newArray(0xB0, 0xB1), newArray(0xC0, 0xC1, 0xC2),
                newArray(0xD0, 0xD1, 0xD2, 0xD3), newArray(0xE0, 0xE1, 0xE2, 0xE3, 0xE4),
                newArray(0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5) };

        assertEquals(versions.length, contents.length);

        // Build expected
        final byte[] expectedArray = new byte[1000];
        expectedArray[0] = (byte) TYPE_MVV;
        Util.putLong(expectedArray, 1, 0);
        Util.putShort(expectedArray, 9, 0);
        int off = 11;

        for (int i = 0; i < versions.length; ++i) {
            Util.putLong(expectedArray, off, versions[i]);
            Util.putShort(expectedArray, off + 8, contents[i].length);
            System.arraycopy(contents[i], 0, expectedArray, off + 10, contents[i].length);
            off += 10 + contents[i].length;
        }

        // Build actual
        int targetLength = 0;
        final byte[] target = new byte[1000];
        for (int i = 0; i < versions.length; ++i) {
            targetLength = storeVersion(target, targetLength, versions[i], contents[i], contents[i].length);
        }

        assertArrayEqualsLen(expectedArray, target, targetLength);
    }

    @Test(expected = IllegalArgumentException.class)
    public void storeToUndefinedOverCapacity() {
        final long vh = 10;
        final byte[] source = { 0xA, 0xB, 0xC };
        final int neededLength = MVV.overheadLength(2) + source.length;
        final byte[] target = new byte[neededLength - 1];

        storeVersion(target, 0, vh, source, source.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void storeToPrimordialOverCapacity() {
        final long vh = 10;
        final byte[] source = { 0xA, 0xB, 0xC };
        final int neededLength = MVV.overheadLength(2) + source.length + 3;

        final byte[] target = new byte[neededLength - 1];
        final int targetLength = writeArray(target, 0xD, 0xE, 0xF);

        storeVersion(target, targetLength, vh, source, source.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void storeToExistingOverCapacity() {
        final long vh1 = 10, vh2 = 11;
        final byte[] source1 = { 0xA, 0xB, 0xC };
        final byte[] source2 = { 0xD, 0xE, 0xF };
        final int neededLength = MVV.overheadLength(3) + source1.length + source2.length;
        final byte[] target = new byte[neededLength - 1];

        int targetLength = 0;
        try {
            targetLength = storeVersion(target, targetLength, vh1, source1, source1.length);
        } catch (IllegalArgumentException e) {
            Assert.fail("Expected success on first store");
        }

        storeVersion(target, targetLength, vh2, source2, source2.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void storeToExistingVersionLongerLengthOverCapacity() {
        final long vh = 10;
        final byte[] source = { 0xA, 0xB, 0xC, 0xD };
        final int neededLength = MVV.overheadLength(2) + source.length;
        final byte[] target = new byte[neededLength - 1];

        int targetLength = 0;
        try {
            targetLength = storeVersion(target, targetLength, vh, source, source.length - 1);
        } catch (IllegalArgumentException e) {
            Assert.fail("Expected success on first store");
        }

        storeVersion(target, targetLength, vh, source, source.length);
    }

    @Test
    public void storeToExistingVersionAtCapacityShorterLength() {
        final long vh = 10;
        final byte[] source = { 0xA, 0xB, 0xC, 0xD };
        final int neededLength = MVV.overheadLength(2) + source.length;
        final byte[] target = new byte[neededLength];

        int targetLength = 0;
        targetLength = storeVersion(target, targetLength, vh, source, source.length);
        storeVersion(target, targetLength, vh, source, source.length - 1);
    }

    @Test
    public void fetchVersionFromUnused() {
        final long vh = 10;
        final byte[] source = {};
        final byte[] target = {};
        assertEquals(MVV.VERSION_NOT_FOUND, MVV.fetchVersion(source, -1, vh, target));
    }

    @Test
    public void fetchVersionFromUndefined() {
        final long vh = 10;
        final byte[] source = {};
        final byte[] target = {};
        assertEquals(MVV.VERSION_NOT_FOUND, MVV.fetchVersion(source, source.length, vh, target));
    }

    @Test
    public void fetchVersionFromPrimordial() {
        final long vh = 10;
        final byte[] source = { 0xA, 0xB, 0xC };
        final byte[] target = {};
        assertEquals(MVV.VERSION_NOT_FOUND, MVV.fetchVersion(source, source.length, vh, target));
    }

    @Test
    public void fetchVersionFromExistingNoFound() {
        final long vh = 10;
        final byte[] source = { (byte) TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 2,
                0, 2, 0xD, 0xE };
        final byte[] target = {};
        assertEquals(MVV.VERSION_NOT_FOUND, MVV.fetchVersion(source, source.length, vh, target));
    }

    @Test
    public void fetchVersionFromExisting() {
        final long vh = 10;
        final byte[] source = { (byte) TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 10, 0, 2, 0xA, 0xB, 0, 0, 0, 0, 0, 0, 0, 11, 0,
                3, 0xB, 0xC };
        final byte[] expected = { 0xA, 0xB };
        final byte[] target = new byte[20];
        final int fetchedLen = MVV.fetchVersion(source, source.length, vh, target);
        assertEquals(expected.length, fetchedLen);
        assertArrayEqualsLen(expected, target, expected.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchVersionFromExistingOverCapacity() {
        final long vh = 10;
        final byte[] source = { (byte) TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 11, 0, 5, 0x1, 0x2, 0x3, 0x4, 0x5, 0, 0, 0, 0, 0,
                0, 0, 9, 0, 1, 0xA, 0, 0, 0, 0, 0, 0, 0, 10, 0, 3, 0xB, 0xC, 0xD };
        final byte[] expected = { 0xB, 0xC, 0xD };
        final byte[] target = new byte[expected.length - 1];
        MVV.fetchVersion(source, source.length, vh, target);
    }

    @Test
    public void visitUnused() throws PersistitException {
        final byte[] source = {};
        TestVisitor visitor = new TestVisitor();
        MVV.visitAllVersions(visitor, source, 0, -1);
        assertTrue(visitor.initCalled);
        assertEquals(newVisitorMap(), visitor.versions);
    }

    @Test
    public void visitUndefined() throws PersistitException {
        final byte[] source = {};
        TestVisitor visitor = new TestVisitor();
        MVV.visitAllVersions(visitor, source, 0, source.length);
        assertTrue(visitor.initCalled);
        assertEquals(newVisitorMap(0, 0, 0), visitor.versions);
    }

    @Test
    public void visitAndFetchByOffsetPrimordial() throws PersistitException {
        final byte[] source = { 0xA, 0xB, 0xC };
        TestVisitor visitor = new TestVisitor();
        MVV.visitAllVersions(visitor, source, 0, source.length);
        assertTrue(visitor.initCalled);
        assertEquals(newVisitorMap(0, 3, 0), visitor.versions);

        final byte[] target = new byte[3];
        MVV.fetchVersionByOffset(source, source.length, 0, target);
        assertArrayEquals(source, target);
    }

    @Test
    public void visitAndFetchByOffsetMVV() throws PersistitException {
        final byte[] source = { (byte) TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 2,
                0, 2, 0xD, 0xE, 0, 0, 0, 0, 0, 0, 0, 11, 0, 5, 0x1, 0x2, 0x3, 0x4, 0x5, 0, 0, 0, 0, 0, 0, 0, 127, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 9, 0, 1, 0xA, 0, 1, 2, 3, 4, 5, 6, 7, 0, 3, 0xB, 0xC, 0xD, 0, 0, 0, 0, 0, 0, 0, 3,
                0, 0 };
        TestVisitor visitor = new TestVisitor();
        MVV.visitAllVersions(visitor, source, 0, source.length);
        assertTrue(visitor.initCalled);
        assertEquals(newVisitorMap(1, 3, 11, 2, 2, 24, 11, 5, 36, 127, 0, 51, 9, 1, 61, 283686952306183L, 3, 72, 3, 0,
                85), visitor.versions);

        for (Map.Entry<Long, LengthAndOffset> entry : visitor.versions.entrySet()) {
            int length = (int) entry.getValue().length;
            int offset = (int) entry.getValue().offset;
            byte[] target = new byte[length];
            MVV.fetchVersionByOffset(source, source.length, offset, target);
            assertArrayEqualsLen(source, offset, target, length);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchByOffsetNegative() {
        final byte[] source = {};
        final byte[] target = new byte[10];
        MVV.fetchVersionByOffset(source, source.length, -1, target);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchByOffsetTooLarge() {
        final byte[] source = newArray(TYPE_MVV, 0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0xA, 0xB, 0xC);
        final byte[] target = new byte[10];
        MVV.fetchVersionByOffset(source, source.length, source.length + 1, target);
    }

    @Test
    public void storeAndFetchVersionMany() {
        final int VERSION_COUNT = 10;
        final int versions[] = new int[VERSION_COUNT];
        final byte sources[][] = new byte[VERSION_COUNT][];

        for (int i = 0; i < VERSION_COUNT; ++i) {
            versions[i] = (i + 1) * 5;
            final int length = (versions[i] % 4 + 1) * 5;
            sources[i] = new byte[length];
            for (int j = 0; j < length; ++j) {
                sources[i][j] = (byte) (2 * j);
            }
        }

        int targetLength = 0;
        final byte target[] = new byte[MVV.overheadLength(VERSION_COUNT) + VERSION_COUNT * 20];
        for (int i = 0; i < VERSION_COUNT; ++i) {
            targetLength = storeVersion(target, targetLength, versions[i], sources[i], sources[i].length);
        }

        final byte fetchtarget[] = new byte[50];
        for (int i = 0; i < VERSION_COUNT; ++i) {
            int fetchedLen = MVV.fetchVersion(target, targetLength, versions[i], fetchtarget);
            assertEquals(sources[i].length, fetchedLen);
            assertArrayEqualsLen(sources[i], fetchtarget, sources[i].length);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void tryValueTooLong() {
        final long VERSION = 10;
        final int LENGTH = MVV.MAX_LENGTH_MASK + 1;
        final byte[] target = new byte[LENGTH + 100];
        final byte[] source = new byte[LENGTH];
        MVV.storeVersion(target, 0, 0, target.length, VERSION, source, 0, source.length);
    }

    //
    // Test helper methods
    //

    private static int writeArray(byte[] array, int... contents) {
        assert contents.length <= array.length : "Too many values for array";
        for (int i = 0; i < contents.length; ++i) {
            int value = contents[i];
            assert value >= 0 && value <= 255 : "Value " + value + " out of byte range at index " + i;
            array[i] = (byte) value;
        }
        return contents.length;
    }

    private static byte[] newArray(int... contents) {
        byte[] array = new byte[contents.length];
        writeArray(array, contents);
        return array;
    }

    private static void assertArrayEqualsLen(byte[] expected, byte[] actual, int length) {
        assertArrayEqualsLen(expected, 0, actual, length);
    }

    private static void assertArrayEqualsLen(byte[] expected, int offset, byte[] actual, int length) {
        if (expected.length < length) {
            throw new AssertionError(String.format("Expected array is too short: %d vs %d", actual.length, length));
        }
        if (actual.length < length) {
            throw new AssertionError(String.format("Actual array is too short: %d vs %d", actual.length, length));
        }
        for (int i = 0; i < length; ++i) {
            byte bE = expected[offset + i];
            byte bA = actual[i];
            if (bE != bA) {
                throw new AssertionError(String.format("Arrays differed at element [%d]: expected <%d> but was <%d>",
                        i, bE, bA));
            }
        }
    }

    private static class LengthAndOffset {
        long length;
        long offset;

        public LengthAndOffset(long length, long offset) {
            this.length = length;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return String.format("(%d,%d)", length, offset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof LengthAndOffset))
                return false;
            LengthAndOffset that = (LengthAndOffset) o;
            return length == that.length && offset == that.offset;
        }
    }

    private static class TestVisitor implements MVV.VersionVisitor {
        boolean initCalled = false;
        Map<Long, LengthAndOffset> versions = new TreeMap<Long, LengthAndOffset>();

        @Override
        public void init() {
            initCalled = true;
            versions.clear();
        }

        @Override
        public void sawVersion(long version, int offset, int valueLength) {
            versions.put(version, new LengthAndOffset(valueLength, offset));
        }
    }

    private static Map<Long, LengthAndOffset> newVisitorMap(long... vals) {
        assertTrue("must be (version,length,offset) triplets", (vals.length % 3) == 0);
        Map<Long, LengthAndOffset> outMap = new TreeMap<Long, LengthAndOffset>();
        for (int i = 0; i < vals.length; i += 3) {
            outMap.put(vals[i], new LengthAndOffset(vals[i + 1], vals[i + 2]));
        }
        return outMap;
    }

    /**
     * Helper method to emulate the original MVV.storeVersion. Now the caller is
     * obligated to called and check the exact required length before calling
     * storeVersion and is likely to get an ArrayIndexOutOfBounds or other
     * undefined behavior if it fails to do so.
     */
    static int storeVersion(byte[] target, int targetLength, long versionHandle, byte[] source, int sourceLength) {
        return MVV.storeVersion(target, 0, targetLength, target.length, versionHandle, source, 0, sourceLength);
    }

}
