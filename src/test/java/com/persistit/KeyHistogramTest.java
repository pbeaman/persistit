/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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
