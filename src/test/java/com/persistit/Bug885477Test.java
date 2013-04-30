/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Suppose a persistit tree has keys (1, 10), (1, 20), (2, 30). If an exchange's
 * key is (1), then traverse(GTEQ, false) should traverse to the first record,
 * resulting in the key still containing (1). Instead, traverse results in an
 * exchange with key (2), as if the EQ part of the direction was ignored.
 */

public class Bug885477Test extends PersistitUnitTestCase {

    @Test
    public void testNotInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();

        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());

        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());
        ex.append(30);
        assertFalse(ex.hasChildren());
        assertTrue(ex.isValueDefined());

    }

    @Test
    public void testInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();

        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());

        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());

        txn.commit();
        txn.end();
    }

}
