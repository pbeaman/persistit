/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import static org.junit.Assert.*;

import org.junit.Test;

import com.persistit.PersistitUnitTestCase;
import com.persistit.Value;

public class ValueTest4 extends PersistitUnitTestCase {


    @Test
    public void streamMode() throws Exception {
        final Value value = new Value(_persistit);
        value.setStreamMode(true);
        value.put(1);
        value.put(2f);
        value.put("abc");
        value.put("abc");
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
        final String s1 = (String)value.get();
        assertEquals("expect String class", String.class, value.getType());
        final String s2 = (String)value.get();
        assertEquals("expect String class", String.class, value.getType());
        final String s3 = (String)value.get();
        assertEquals("expect value", s1, "abc");
        assertEquals("expect value", s2, "abc");
        assertEquals("expect value", s3, "abc");
        assertEquals("expect Long class", Long.class, value.getType());
        final Long l1 = (Long)value.get();
        assertEquals("expect Long class", Long.class, value.getType());
        final Long l2 = (Long)value.get();
        assertEquals("expect equal values", l1, l2);
        assertTrue("encoding of primitive wrapper classes loses identity", l1 == l2);
        assertTrue ("interned constant \"abc\" has same identity", s1 == s2);
        assertTrue ("computed object \"xxabc\".substring(2) has different identity", s1 != s3);
    }
}
