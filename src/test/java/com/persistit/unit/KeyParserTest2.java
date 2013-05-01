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
import com.persistit.KeyFilter;
import com.persistit.KeyParser;
import com.persistit.PersistitUnitTestCase;

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
