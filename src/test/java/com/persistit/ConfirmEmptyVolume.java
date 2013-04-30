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

import com.persistit.stress.AbstractStressTest;
import com.persistit.stress.TestResult;

public class ConfirmEmptyVolume extends AbstractStressTest {

    Volume[] _volumes;

    public ConfirmEmptyVolume(final String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (_args.length == 0) {
            _args = new String[] { "persistit" };
        }

        _volumes = new Volume[_args.length];

        for (int index = 0; index < _args.length; index++) {
            final Volume volume = getPersistit().getVolume(_args[index]);
            if (volume == null) {
                System.out.println("Volume name not found: " + _args[index]);
            } else {
                _volumes[index] = volume;
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        _volumes = null;
        super.tearDown();
    }

    public void confirmEmpty() {
        final TestResult[] results = new TestResult[_volumes.length];
        int resultCount = 0;
        boolean passed = true;
        try {
            for (int index = 0; index < _volumes.length; index++) {
                final Volume volume = _volumes[index];
                if (volume != null) {
                    final String[] treeNames = volume.getTreeNames();
                    final boolean empty = treeNames.length == 0;
                    if (empty) {
                        results[resultCount] = new TestResult(true, "no trees");
                    } else {
                        final StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < treeNames.length; i++) {
                            sb.append(treeNames[i]);
                            sb.append(" ");
                        }
                        results[resultCount] = new TestResult(false, sb.toString());
                        passed = false;
                    }
                    resultCount++;
                } else {
                    fail("Volume name " + _args[index] + " not found");
                    results[index] = _result;
                    resultCount++;
                }
            }
            if (resultCount > 1) {
                _result = new TestResult(passed, results.toString());
            }

        } catch (final Exception ex) {
            fail(ex);
        }
    }

    @Override
    public void executeTest() throws Exception {
        confirmEmpty();
    }
}
