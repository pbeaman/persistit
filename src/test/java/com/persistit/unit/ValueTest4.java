/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.PersistitUnitTestCase;
import com.persistit.Value;

public class ValueTest4 extends PersistitUnitTestCase {

    private final static String ABC = "abc";

    @Test
    public void streamMode() throws Exception {
        final Value value = new Value(_persistit);
        value.setStreamMode(true);
        value.put(1);
        value.put(2f);
        value.put(ABC);
        value.put(ABC);
        value.put("xxabc".substring(2));
        value.put(new Long(5));
        value.put(new Long(5));
        value.setStreamMode(false);
        value.setStreamMode(true);
        assertEquals("expect primitive int class", int.class, value.getType());
        assertEquals("expect value", 1, value.get());
        assertEquals("expect primitive float class", float.class, value.getType());
        assertEquals("expect value", 2f, value.get());
        assertEquals("expect String class", String.class, value.getType());
        final String s1 = (String) value.get();
        assertEquals("expect String class", String.class, value.getType());
        final String s2 = (String) value.get();
        assertEquals("expect String class", String.class, value.getType());
        final String s3 = (String) value.get();
        assertEquals("expect value", ABC, s1);
        assertEquals("expect value", ABC, s2);
        assertEquals("expect value", ABC, s3);
        assertEquals("expect Long class", Long.class, value.getType());
        final Long l1 = (Long) value.get();
        assertEquals("expect Long class", Long.class, value.getType());
        final Long l2 = (Long) value.get();
        assertEquals("expect equal values", l1, l2);
        assertTrue("encoding of primitive wrapper classes loses identity", l1 == l2);
        assertTrue("interned constant \"abc\" has same identity", s1 == s2);
        assertTrue("computed object \"xxabc\".substring(2) has different identity", s1 != s3);
    }

    @Test
    public void streamModeSkipNull() throws Exception {
        final Value value = new Value(_persistit);
        value.setStreamMode(true);
        value.put(1);
        value.put(2);
        value.put(null);
        value.put(4);
        value.put(null);
        value.put(null);
        value.put(null);
        value.put(8);
        value.setStreamMode(false);
        value.setStreamMode(true);
        assertEquals("expected value of field 1", 1, value.get());
        assertEquals("expected value of field 2", 2, value.get());
        assertTrue("field 3 is null, don't advance cursor", value.isNull());
        assertTrue("field 3 is null, don't advance cursor", value.isNull());
        assertTrue("field 3 is null, don't advance cursor", value.isNull());
        assertTrue("field 3 is null, do advance cursor", value.isNull(true));
        assertTrue("should be field 4", !value.isNull());
        assertTrue("should be field 4", !value.isNull(true));
        assertEquals("expected value of field 4", 4, value.getInt());
        assertTrue("field 5 should be null", value.isNull(true));
        assertTrue("field 6 should be null", value.isNull(true));
        assertTrue("field 7 should be null", value.isNull(true));
        assertTrue("field 8 should not be null", !value.isNull(true));
        assertTrue("field 8 should not be null", !value.isNull(true));
        assertTrue("field 8 should not be null", !value.isNull(true));
        assertTrue("field 8 should not be null", !value.isNull(true));
        assertEquals("expected value of field 8", 8, value.get());
    }

    @Test
    public void streamModeGetAfterSkip() throws Exception {
        final Value value = new Value(_persistit);
        value.setStreamMode(true);
        // All same instance due to constant intern
        value.put(ABC);
        value.put(2);
        value.put(ABC);
        value.put(4);
        value.put(ABC);
        value.put(6);
        value.put(ABC);
        value.put(8);
        value.put(ABC);
        value.put(ABC);
        value.put(ABC);
        value.setStreamMode(false);
        value.setStreamMode(true);
        value.skip(); // "abc"
        assertEquals("expect 2", 2, value.getInt());
        value.skip(); // "abc"
        assertEquals("expect 2", 4, value.getInt());

        assertEquals("Field 5 should be a String", String.class, value.getType());
        final String s5 = value.getString();
        assertEquals("expect value", ABC, s5);

        assertEquals("expect 6", 6, value.getInt());

        assertEquals("Field 7 should be a String", String.class, value.getType());
        final String s7 = value.getString();
        assertTrue("expect identical", s5 == s7);
    }

    /**
     * See https://bugs.launchpad.net/akiban-persistit/+bug/1081659
     * 
     * @throws Exception
     */
    @Test
    public void serializedItemCount() throws Exception {
        final Value value = new Value(_persistit);
        value.setStreamMode(true);
        value.put(null);
        value.put(ABC);
        value.put(null);
        value.put(ABC);
        value.setStreamMode(false);
        value.setStreamMode(true);
        assertTrue("Expect null", value.isNull(true));
        assertEquals("Don't expect null", ABC, value.get());
        assertTrue("Expect null", value.isNull(true));
        assertEquals("Expect String", String.class, value.getType());
        assertEquals("Don't expect null", ABC, value.get());
    }

}
