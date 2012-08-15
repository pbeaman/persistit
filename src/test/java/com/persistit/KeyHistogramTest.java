/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KeyHistogramTest extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    @Test
    public void testCull() throws Exception {
        final int total = 1000;
        final int samples = 10;
        final KeyHistogram histogram = new KeyHistogram(null, null, null, samples, 1, 1);
        final Key key = new Key((Persistit) null);
        for (int i = 0; i < total; i++) {
            key.to(i);
            histogram.addKeyCopy(key);
        }
        histogram.cull();
        assertEquals(total, histogram.getKeyCount());
        assertEquals(samples, histogram.getSampleSize());
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
    }

    @Test
    public void testGather() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestGather", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            exchange.to(i).store();
        }
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, null, 1);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(samples, histogram.getSampleSize());
    }

    @Test
    public void testUnequalDistribution() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestUnequalDistribution", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            final String which = i % 100 < 10 ? "A" : i % 100 < 90 ? "B" : "C";
            exchange.clear().append(which).append(i).store();
        }
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, null, 0);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(3, histogram.getSampleSize());
    }

    @Test
    public void testFiltered() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestFiltered", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            exchange.clear().append(i);
            if ((i % 100) == 0) {
                exchange.store();
            }
            exchange.append(i).store();
        }
        final KeyFilter keyFilter = new KeyFilter(new KeyFilter.Term[] { KeyFilter.ALL }, 0, 1);
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, keyFilter, 0);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(samples, histogram.getSampleSize());
    }

    @Override
    public void runAllTests() throws Exception {
        testCull();
        testGather();
        testUnequalDistribution();
        testFiltered();
    }

}
