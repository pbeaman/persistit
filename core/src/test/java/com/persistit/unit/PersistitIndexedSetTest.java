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

package com.persistit.unit;

import com.persistit.Exchange;
import com.persistit.PersistitIndexedSet;
import com.persistit.exception.PersistitException;

import java.util.HashSet;

public class PersistitIndexedSetTest extends PersistitUnitTestCase {
    private final static String VOLUME_NAME = "persistit";
    private final static String TREE_NAME = "IndexedSetTest";
    private final Object[] TEST_OBJECTS = {false, 1, (long)10, 'a', "bob"};

    private PersistitIndexedSet createIndexedSet() throws PersistitException {
        Exchange ex = _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
        return new PersistitIndexedSet(ex);
    }

    public void testPut() throws Exception {
        HashSet<Long> idSet = new HashSet<Long>();
        PersistitIndexedSet indexedSet = createIndexedSet();
        for(Object o : TEST_OBJECTS) {
            long id = indexedSet.put(o);
            assertFalse("id for '" + o + "' not unique: " + id, idSet.contains(id));
            idSet.add(id);
        }
    }

    public void testIsDefined_id() throws Exception {
        PersistitIndexedSet indexedSet = createIndexedSet();
        for(Object o : TEST_OBJECTS) {
            long id = indexedSet.put(o);
            assertTrue("id for '" + o + "' not defined: " + id, indexedSet.isDefined(id));
        }
    }

    public void testIsDefined_object() throws Exception {
        PersistitIndexedSet indexedSet = createIndexedSet();
        for(Object o : TEST_OBJECTS) {
            assertFalse("object defined pre-put: " + o, indexedSet.isDefined(o));
            indexedSet.put(o);
            assertTrue("object not defined post-put: " + o, indexedSet.isDefined(o));
        }
    }

    public void testLookup_id() throws Exception {
        PersistitIndexedSet indexedSet = createIndexedSet();
        for(Object o : TEST_OBJECTS) {
            long id = indexedSet.put(o);
            assertEquals("lookup '" + o + "' by id: " + id, o, indexedSet.lookup(id));
        }
    }

    public void testLookup_object() throws Exception {
        PersistitIndexedSet indexedSet = createIndexedSet();
        for(Object o : TEST_OBJECTS) {
            long id = indexedSet.put(o);
            assertEquals("lookup id for: " + o, id, indexedSet.lookup(o));
        }
    }
    
    public void testReplace() throws Exception {
        final Object[] replacedObjects = {'q', 'w', 'e', 'r', 't'};
        long[] objectIDs = new long[replacedObjects.length];
        PersistitIndexedSet indexedSet = createIndexedSet();

        assertEquals("object array sizes", TEST_OBJECTS.length, replacedObjects.length);

        for(int i = 0; i < TEST_OBJECTS.length; ++i) {
            objectIDs[i] = indexedSet.put(TEST_OBJECTS[i]);
        }

        for(int i = 0 ; i < replacedObjects.length; ++i) {
            indexedSet.replace(objectIDs[i], replacedObjects[i]);
        }

        for(int i = 0; i < TEST_OBJECTS.length; ++i) {
            long id = objectIDs[i];
            Object orig = TEST_OBJECTS[i];
            Object replaced = replacedObjects[i];
            assertTrue("id is defined: " + id, indexedSet.isDefined(id));
            assertFalse("original object is defined: " + orig, indexedSet.isDefined(orig));
            assertTrue("replaced object is defined: " + replaced, indexedSet.isDefined(replaced));
        }
    }

    public void testRemoveAll() throws Exception {
        PersistitIndexedSet indexedSet = createIndexedSet();

        long[] objectIDs = new long[TEST_OBJECTS.length];
        for(int i = 0; i < TEST_OBJECTS.length; ++i) {
            objectIDs[i] = indexedSet.put(TEST_OBJECTS[i]);
        }

        boolean expected = true;
        for(int i = 0; i < 3; ++i) {
            for(int j = 0; j < TEST_OBJECTS.length; ++j) {
                long id = objectIDs[j];
                Object o = TEST_OBJECTS[j];
                assertEquals("iteration " + i + " object is defined: " + o, expected, indexedSet.isDefined(id));
                assertEquals("iteration " + i + " id is defined: " + o, expected, indexedSet.isDefined(o));
            }
            indexedSet.removeAll();
            expected = false;
        }
    }
}
