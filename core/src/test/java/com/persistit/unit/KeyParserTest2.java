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
import com.persistit.KeyFilter;
import com.persistit.KeyParser;

public class KeyParserTest2 extends PersistitUnitTestCase {

    @Test
    public void test1() {
        System.out.print("test1 ");
        final KeyFilter filter = new KeyFilter(new KeyFilter.Term[] { KeyFilter.rangeTerm("a", "b"),
                KeyFilter.rangeTerm(new Double(1.234), new Double(2.345)), KeyFilter.simpleTerm("{{{,,,}}}") });

        final String string1 = filter.toString();
        // System.out.println();
        // System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        // System.out.println("filter2.toString()=" + string2);
        assertEquals(string1, string2);
        System.out.println("- done");
    }

    @Test
    public void test2() {
        System.out.print("test2 ");
        final KeyFilter filter = new KeyFilter(new KeyFilter.Term[] {
                KeyFilter.rangeTerm("a", "b"),
                KeyFilter.rangeTerm(new Double(1.234), new Double(2.345)),
                KeyFilter.simpleTerm("{{{,,,}}}"),
                KeyFilter.orTerm(new KeyFilter.Term[] {
                        KeyFilter.rangeTerm(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE)),
                        KeyFilter.simpleTerm("1st"), KeyFilter.simpleTerm("2nd"), }) }, 3, 7);

        final String string1 = filter.toString();
        // System.out.println();
        // System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        // System.out.println("filter2.toString()=" + string2);
        assertEquals(string1, string2);
        System.out.println("- done");
    }

    @Test
    public void test3() {
        System.out.print("test3 ");
        final KeyFilter filter = new KeyFilter(new KeyFilter.Term[] {
                KeyFilter.rangeTerm("a", "b"),
                KeyFilter.rangeTerm(new Double(1.234), new Double(2.345)),
                KeyFilter.simpleTerm("{{{,,,}}}"),
                KeyFilter.orTerm(new KeyFilter.Term[] {
                        KeyFilter.rangeTerm(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE)),
                        KeyFilter.simpleTerm("1st"), KeyFilter.simpleTerm("2nd"), }) }, 3, 7);

        final String string1 = filter.toString();
        // System.out.println();
        // System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        // System.out.println("filter2.toString()=" + string2);
        assertEquals(string1, string2);
        System.out.println("- done");
    }

    @Test
    public void test4() {
        System.out.println("test4 ");
        final KeyParser parser = new KeyParser("{*,>100:200,*<}");
        final KeyFilter filter = parser.parseKeyFilter();
        assertEquals(filter.getMinimumDepth(), 2);
        assertEquals(filter.getMaximumDepth(), 3);
        assertEquals(filter.size(), 3);
        assertEquals(filter.getTerm(0), KeyFilter.ALL);
        assertEquals(filter.getTerm(1), KeyFilter.rangeTerm(new Integer(100), new Integer(200)));
        assertEquals(filter.getTerm(2), KeyFilter.ALL);
        final Key key = new Key(_persistit);
        assertTrue(!filter.selected(key));
        key.append("x");
        assertTrue(!filter.selected(key));
        key.append(150);
        assertTrue(filter.selected(key));
        key.append("y");
        assertTrue(filter.selected(key));
        key.append("z");
        assertTrue(!filter.selected(key));
        key.append("zz");
        assertTrue(!filter.selected(key));
        key.setDepth(2);
        assertTrue(filter.selected(key));
        key.to(50);
        assertTrue(filter.next(key, Key.GT));
        key.to(120);
        assertTrue(filter.next(key, Key.GT));
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new KeyParserTest2().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        setUp();
        test1();
        test2();
        test3();
        test4();

    }

}
