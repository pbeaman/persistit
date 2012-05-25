/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
