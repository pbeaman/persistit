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

import org.junit.Test;

import com.persistit.Key;
import com.persistit.KeyParser;
import com.persistit.Persistit;

/**
 * @version 1.0
 */
public class KeyParserTest1 extends PersistitUnitTestCase {

    private static final Persistit _persistit = new Persistit();

    public static String[] VALID_STRINGS = {
            "{1}",
            "{3.14159265,-123,-456,-7.891,-0.1,0.01,-0.01}",
            "{(byte)123,(byte)-123,true,null,false,(char)456}",
            "{\"abc\",\"456\",\"def\"}",
            // "{(Date)20040820082356.123-0400,(float)1.0,1.0}",
            "{\"\\b\\t\\r\\n\\\\\\\"\",\"\\\\\\\"\",\"\\u45AD\\u0000\\u0001\\u0002\"}",
    // {"\b\t\r\n\\\"","\\\"","\u45AD\U0000\U0001\u0002"}
    };

    public static String[] INVALID_STRINGS = {};

    @Test
    public void test1() {
        final Key key = new Key(_persistit);
        System.out.print("test1 ");
        for (int i = 0; i < VALID_STRINGS.length; i++) {
            // System.out.println(i + " " + VALID_STRINGS[i]);
            final KeyParser kp = new KeyParser(VALID_STRINGS[i]);
            key.clear();
            final boolean result = kp.parseKey(key);
            // System.out.println("  " + result + "  " + key.toString());
            assertTrue(result);
            assertEquals(VALID_STRINGS[i], key.toString());
        }
        System.out.println("- done");
    }

    @Test
    public void test2() {
        System.out.print("test2 ");
        System.out.println("- done");
    }

    // public void test3()
    // throws PersistitException
    // {
    // System.out.print("test3 ");
    // }
    //
    // public void test4()
    // throws PersistitException
    // {
    // System.out.print("test4 ");
    // System.out.println("- done");
    // }

    public static void main(final String[] args) throws Exception {
        new KeyParserTest1().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
        test2();
    }

}
