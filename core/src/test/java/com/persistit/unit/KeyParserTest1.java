/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.Key;
import com.persistit.KeyParser;
import com.persistit.Persistit;

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
