/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 15, 2004
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
//            System.out.println(i + " " + VALID_STRINGS[i]);
            final KeyParser kp = new KeyParser(VALID_STRINGS[i]);
            key.clear();
            final boolean result = kp.parseKey(key);
//            System.out.println("  " + result + "  " + key.toString());
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
