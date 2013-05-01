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

import com.persistit.Key;
import com.persistit.KeyParser;
import com.persistit.Persistit;
import com.persistit.PersistitUnitTestCase;

/**
 * @version 1.0
 */
public class KeyParserTest1 extends PersistitUnitTestCase {

    private static final Persistit _persistit = new Persistit();

    public static String[] VALID_STRINGS = { "{1}", "{3.14159265,-123,-456,-7.891,-0.1,0.01,-0.01}",
            "{(byte)123,(byte)-123,true,null,false,(char)456}", "{\"abc\",\"456\",\"def\"}",
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

    public static void main(final String[] args) throws Exception {
        new KeyParserTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
        test2();
    }

}
