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

public class ConfirmIntegrity extends AbstractStressTest {

    Volume[] _volumes;
    IntegrityCheck[] _ichecks;
    int _icheckIndex = -1;

    public ConfirmIntegrity(final String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
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
        _ichecks = null;
        super.tearDown();
    }

    public void checkIntegrity() {
        _ichecks = new IntegrityCheck[_volumes.length];
        final TestResult[] results = new TestResult[_volumes.length];
        int resultCount = 0;
        boolean passed = true;
        try {
            for (_icheckIndex = 0; _icheckIndex < _volumes.length; _icheckIndex++) {
                final Volume volume = _volumes[_icheckIndex];
                if (volume != null) {
                    final IntegrityCheck icheck = new IntegrityCheck(getPersistit());
                    _ichecks[_icheckIndex] = icheck;

                    icheck.checkVolume(volume);
                    _result = new TestResult(!icheck.hasFaults(), icheck.toString());

                    results[_icheckIndex] = _result;

                    if (icheck.hasFaults()) {
                        passed = false;
                    }
                    resultCount++;
                } else {
                    _result = new TestResult(false, "Volume name " + _args[_icheckIndex] + " not found");
                    results[_icheckIndex] = _result;
                    resultCount++;
                }
            }
            if (resultCount > 1) {
                _result = new TestResult(passed, results.toString());
            }

        } catch (final Exception ex) {
            _result = new TestResult(false, ex);
        }
    }

    @Override
    public void executeTest() throws Exception {
        checkIntegrity();
    }

}
