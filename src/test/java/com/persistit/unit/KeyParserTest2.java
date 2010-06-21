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
import com.persistit.KeyFilter;
import com.persistit.KeyParser;

public class KeyParserTest2 extends PersistitUnitTestCase {

    @Test
    public void test1() {
        System.out.print("test1 ");
        final KeyFilter filter = new KeyFilter(new KeyFilter.Term[] {
                KeyFilter.rangeTerm("a", "b"),
                KeyFilter.rangeTerm(new Double(1.234), new Double(2.345)),
                KeyFilter.simpleTerm("{{{,,,}}}") });

        final String string1 = filter.toString();
        System.out.println();
        System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        System.out.println("filter2.toString()=" + string2);
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
                        KeyFilter.rangeTerm(new Long(Long.MIN_VALUE), new Long(
                                Long.MAX_VALUE)), KeyFilter.simpleTerm("1st"),
                        KeyFilter.simpleTerm("2nd"), }) }, 3, 7);

        final String string1 = filter.toString();
        System.out.println();
        System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        System.out.println("filter2.toString()=" + string2);
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
                        KeyFilter.rangeTerm(new Long(Long.MIN_VALUE), new Long(
                                Long.MAX_VALUE)), KeyFilter.simpleTerm("1st"),
                        KeyFilter.simpleTerm("2nd"), }) }, 3, 7);

        final String string1 = filter.toString();
        System.out.println();
        System.out.println("filter1.toString()=" + string1);
        final KeyParser parser = new KeyParser(string1);
        final KeyFilter filter2 = parser.parseKeyFilter();
        assertTrue(filter2 != null);
        final String string2 = filter2.toString();
        System.out.println("filter2.toString()=" + string2);
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
        assertEquals(filter.getTerm(1), KeyFilter.rangeTerm(new Integer(100),
                new Integer(200)));
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
        assertTrue(filter.traverse(key, true));
        key.to(120);
        assertTrue(filter.traverse(key, true));
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new KeyParserTest2().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        setUp();
        test1();
        test2();
        test3();
        test4();

    }

}
