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

import org.junit.Test;

public class Bug739533Test extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub
    }

    @Test
    public void test739544() throws Exception {

        final Exchange exchange = _persistit.getExchange(_volumeName, "Bug739533", true);

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            sb.append(".");
        }
        final int maxSimpleValueSize = maxValueSize(exchange);
        final int pageSize = exchange.getVolume().getPageSize();

        System.out.println("maxValueSize=" + maxSimpleValueSize);

        for (int k = 100; k < 1990; k += 100) {
            exchange.removeAll();

            for (int i = 0; i < maxSimpleValueSize - 1; i++) {
                sb.append(".");
            }

            exchange.getValue().put(sb.toString().substring(0, pageSize / 2 - 50));

            for (int j = 0; j < 4; j += 2) {
                exchange.clear().append(j);
                exchange.store();
            }

            exchange.getValue().put(sb.toString().substring(0, maxSimpleValueSize - 10));

            for (int j = 1; j < 3; j += 2) {
                exchange.clear().append(j).append(sb.toString().substring(0, k));
                exchange.store();
            }
        }

    }

    /**
     * This is the old computation. The intent is to pick a record that really
     * is too big so that it will exploit the bug.
     * 
     * @param exchange
     * @return
     */
    int maxValueSize(final Exchange exchange) {
        return exchange.getVolume().getPageSize() - Buffer.INDEX_PAGE_OVERHEAD
                - Key.maxStorableKeySize(exchange.getVolume().getPageSize()) * 2;
    }

}
