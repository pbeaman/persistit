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

package com.persistit.bug;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.SplitPolicy;
import com.persistit.unit.PersistitUnitTestCase;

/*
 * Stress10 with 1K pages exhibited a failure in which a value was simply not
 * inserted into the page. Apparently this is due to a miscalculation of free
 * space after a page split. I think this is probably related to, or the same
 * bug as, the former #102 page split error.
 */

public class Bug706132Test extends PersistitUnitTestCase {

    /*
     * {"stress10",637545,7} {"stress10",637567,4} {"stress10",637593,11}
     * {"stress10",637618,6} {"stress10",637701,2} {"stress10",637715,11}
     * {"stress10",637734,9}
     * 
     * {"stress10",637697,0} "test length=417
     */
    @Override
    public void setUp() throws Exception {
        final Properties p = getProperties(true);
        p.setProperty("buffer.count.1024", "20");
        p.remove("buffer.count.8192");
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:1024,initialPages:100,extensionPages:100,"
                + "maximumPages:25000");
        _persistit.initialize(p);
    }

    @Test
    public void test1() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug70612",
                true);
        StringBuilder sb = new StringBuilder();
        ex.removeAll();
        ex.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        sb.setLength(100);
        ex.getValue().put(sb.toString());
        for (int i = 0; i < 8; i++) {
            ex.clear().append(i).store();
        }
        sb.setLength(900);
        ex.getValue().put(sb.toString());
        ex.clear().append("stress10").append(637545).append(7).store();
        ex.clear().append("stress10").append(637567).append(4).store();
        ex.clear().append("stress10").append(637593).append(11).store();
        ex.clear().append("stress10").append(637618).append(6).store();
        ex.clear().append("stress10").append(637701).append(2).store();
        ex.clear().append("stress10").append(637715).append(11).store();
        ex.clear().append("stress10").append(637734).append(9).store();
        ex.clear().append("stress10").append(637741).append(1).store();

        ex.setSplitPolicy(SplitPolicy.NICE_BIAS);

        sb.setLength(416);
        ex.getValue().put(sb.toString());
        ex.clear().append("stress10").append(637697).append(0).store();
        ex.getValue().clear();
        ex.fetch();
        assertTrue(ex.getValue().getString().length() == 416);
    }

    @Test
    public void test2() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug70612",
                true);
        StringBuilder sb = new StringBuilder();
        ex.removeAll();
        ex.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        sb.setLength(100);
        ex.getValue().put(sb.toString());
        for (int i = 0; i < 8; i++) {
            ex.clear().append(i).store();
        }
        sb.setLength(900);
        ex.getValue().put(sb.toString());
        ex.clear().append("stress10").append(521479).append(8).store();
        ex.clear().append("stress10").append(521482).append(8).store();
        ex.clear().append("stress10").append(521485).append(4).store();
        ex.clear().append("stress10").append(521490).append(4).store();
        sb.setLength(321);
        ex.getValue().put(sb.toString());
        ex.clear().append("stress10").append(521491).append(7).store();
        ex.getValue().clear();
        ex.clear().append("stress10").append(521492).append(7).store();

        ex.setSplitPolicy(SplitPolicy.NICE_BIAS);

        sb.setLength(427);
        ex.getValue().put(sb.toString());
        ex.clear().append("stress10").append(521491).append(0).store();
        ex.getValue().clear();
        ex.fetch();
        assertTrue(ex.getValue().getString().length() == 427);
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
        test2();
    }

}
