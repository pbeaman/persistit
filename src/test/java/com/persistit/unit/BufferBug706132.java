package com.persistit.unit;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.SplitPolicy;

public class BufferBug706132 extends PersistitUnitTestCase  {

    /*
     *   {"stress10",637545,7}
     *   {"stress10",637567,4}
     *   {"stress10",637593,11}
     *   {"stress10",637618,6}
     *   {"stress10",637701,2}
     *   {"stress10",637715,11}
     *   {"stress10",637734,9}
     *  
     *   {"stress10",637697,0}
     *   "test length=417
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
    public void testBug70612() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit",
                "bug70612", true);
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

    @Override
    public void runAllTests() throws Exception {
        testBug70612();
    }

}
