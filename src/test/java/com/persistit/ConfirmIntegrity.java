/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import com.persistit.suite.AbstractStressTest;
import com.persistit.suite.TestResult;

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
