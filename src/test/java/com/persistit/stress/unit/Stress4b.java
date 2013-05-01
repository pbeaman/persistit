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

package com.persistit.stress.unit;

import com.persistit.exception.PersistitException;

public class Stress4b extends Stress4Base {

    public Stress4b(final String argsString) {
        super(argsString);
    }

    @Override
    public void repeatedTasks() throws PersistitException {
        writeRecords(_total, false, 1000, 1000);
        readRecords(_total, false, 1000, 1000);
        testForward();
        testReverse();
        testReads(_total);
        seed(_seed);
        writeRecords(_total, true, 20, 1000);
        seed(_seed);
        readRecords(_total, true, 20, 1000);
        seed(_seed * 7);
        writeRecords(_total, true, 20, 8000);
        seed(_seed * 7);
        readRecords(_total, true, 20, 8000);
        testReverse();
        seed(_seed * 7);
        writeRecords(_total / 200, true, 20, 80000);
        seed(_seed * 7);
        readRecords(_total / 200, true, 20, 80000);
        testReverse();
        seed(_seed * 17);
        removeRecords(_seed, true);
        testForward();
    }

}
