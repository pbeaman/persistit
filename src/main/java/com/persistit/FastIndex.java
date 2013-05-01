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

import com.persistit.util.Debug;

/**
 * FastIndex is a wrapper for an array of integers that contain information used
 * in searching for a key in a page. There is one meaningful element in that
 * array for each key block in the corresponding page. FastIndex has a reference
 * to Buffer and vice versa. However, to conserve memory the BufferPool may
 * maintain a smaller number of FastIndex instances than Buffer indexes. When a
 * Buffer needs a FastIndex, {@link BufferPool#allocFastIndex()} method steals a
 * FastIndex from some other Buffer where it currently isn't needed.
 * <p />
 * FastIndex is based on the ebc (elided byte count) of keys in the page. Its
 * purpose is to allow the key search algorithm skip over irrelevant keys having
 * larger ebc values, or when a "run" of keys has the same ebc value, to perform
 * a binary search.
 * <p />
 * An element in the array denotes either one plus the number of successor key
 * blocks with deeper elision (larger ebc values) having the same ebc (elided
 * byte count) or the number of successor key block having the same ebc value.
 * In the first case the integer value is called the "cross count" (number of
 * key blocks to cross before arriving at another key block have a smaller or
 * equal ebc value). In the second case it called the "run count" (number of key
 * blocks in a run having identical ebc values). To distinguish this cases in
 * the array, cross counts are stored as negative numbers while run counts are
 * positive.
 * <p />
 * Consider a page having the following sequence of keys. The ebc value for each
 * key indicates the number of bytes in that key which match the preceding key.
 * Non-zero values in the crossCount and runCount fields indicate values
 * represented the by the FastIndex array for this page.
 * <table>
 * <colgroup> <col align="right"/> <col align="left"/> <col align="right"/> <col
 * align="right"/> <col align="right"/> </colgroup> <thead>
 * <th>index</th>
 * <th>key</th>
 * <th>ebc</th>
 * <th>crossCount
 * <th>runCount </thead>
 * <tr>
 * <td>0</td>
 * <td>A</td>
 * <td>0</td>
 * <td>4</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>1</td>
 * <td>ABC</td>
 * <td>1</td>
 * <td>2</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>ABK</td>
 * <td>2</td>
 * <td></td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>3</td>
 * <td>ABZ</td>
 * <td>2</td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>4</td>
 * <td>AC</td>
 * <td>1</td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>5</td>
 * <td>B</td>
 * <td>0</td>
 * <td></td>
 * <td>2</td>
 * </tr>
 * <tr>
 * <td>6</td>
 * <td>C</td>
 * <td>0</td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>7</td>
 * <td>D</td>
 * <td>0</td>
 * <td>1</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>8</td>
 * <td>DA</td>
 * <td>1</td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>9</td>
 * <td>E</td>
 * <td>0</td>
 * <td></td>
 * <td></td>
 * </tr>
 * </table>
 * <p />
 * The key search algorithm uses these values to accelerate searching for a key
 * in the page. For example, suppose the key being sought is "D". Then the
 * {@link Buffer#findKey(Key)} starts by comparing "D" with A, determines that
 * the elision could of the key block for "D" must be zero, and is able to skip
 * forward to key #5 ("B"). Now starting at key #5, findKey sees that there are
 * three keys having an ebc value of 0, and does a binary search to look for
 * "D".
 * 
 * @author peter
 * 
 */

class FastIndex {

    final static int BYTES_PER_ENTRY = 2;
    /**
     * The Findex array. One element per keyblock holds the crossCount,
     * runCount, ebc and db from the keyblock.
     */
    private final short[] _findexElements;
    /**
     * Indicates whether the _findexElements array is valid
     */
    private boolean _isValid;

    /**
     * The buffer this fast index is associated with.
     */
    private final Buffer _buffer;

    FastIndex(final Buffer buffer, final int elementSize) {
        _buffer = buffer;
        _findexElements = new short[elementSize];
        _isValid = false;
    }

    int size() {
        return _findexElements.length;
    }

    void putRunCount(final int index, final int runCount) {
        _findexElements[index] = (short) runCount;
    }

    int getRunCount(final int index) {
        return _findexElements[index];
    }

    int getEbc(final int index) {
        final int kbData = _buffer.getInt((index << 2) + Buffer.KEY_BLOCK_START);
        final int ebc = Buffer.decodeKeyBlockEbc(kbData);
        return ebc;
    }

    boolean isValid() {
        return _isValid;
    }

    void invalidate() {
        _isValid = false;
    }

    void recompute() {
        if (_buffer.isDataPage() || _buffer.isIndexPage()) {
            final int start = _buffer.getKeyBlockStart();
            final int end = _buffer.getKeyBlockEnd();

            int ebc0 = 0;
            int runCountFixupIndex = 0;
            int crossCountFixupIndex = -1;

            final int lastIndex = (end - start) / Buffer.KEYBLOCK_LENGTH;

            for (int i = 0, p = start; i <= lastIndex; i++, p += Buffer.KEYBLOCK_LENGTH) {
                int ebc;
                if (i < lastIndex || i == 0) {
                    final int kbData = _buffer.getInt(p);
                    ebc = Buffer.decodeKeyBlockEbc(kbData);
                } else {
                    ebc = -1;
                }

                if (ebc != ebc0) {
                    final int runCount = i - runCountFixupIndex - 1;
                    putRunCount(runCountFixupIndex, runCount);
                    runCountFixupIndex = i;

                    if (ebc > ebc0) {
                        //
                        // If not true then the ebc for the very first KeyBlock
                        // is non-zero, which is wrong.
                        //
                        putRunCount(i - 1, crossCountFixupIndex);
                        crossCountFixupIndex = i - 1;
                    } else { // ebc < ebc0 */
                        //
                        // Now we need to walk back through the linked list of
                        // findex array elements that need to have their
                        // crossCount field updated.
                        //
                        for (int j = crossCountFixupIndex; j != -1;) {
                            final int ccFixupEbc = getEbc(j);

                            if (ebc <= ccFixupEbc) {
                                final int crossCount = -(i - j - 1);

                                crossCountFixupIndex = getRunCount(j);
                                putRunCount(j, crossCount);
                                j = crossCountFixupIndex;
                            } else {
                                crossCountFixupIndex = j;
                                break;
                            }

                        }
                    }

                    ebc0 = ebc;
                } else {
                    putRunCount(i, 0);
                }
            }
            if (Debug.ENABLED) {
                verify();
            }
            _isValid = true;
        } else {
            _isValid = false;
        }
    }

    /**
     * @return true iff the index is valid; false otherwise
     */
    boolean verify() {
        if (!isValid()) {
            return false;
        }
        final int start = _buffer.getKeyBlockStart();
        final int end = _buffer.getKeyBlockEnd();
        //
        // This is the index of a Findex array entry that needs to be fixed up
        // with its true count.
        //

        int ebc0;
        int ebc = -1;
        int ebc2 = Buffer.decodeKeyBlockEbc(_buffer.getInt(start));
        int faultAt = -1;

        for (int i = 0, p = start; p < end; i++, p += Buffer.KEYBLOCK_LENGTH) {
            ebc0 = ebc;
            ebc = ebc2;
            ebc2 = p + Buffer.KEYBLOCK_LENGTH < end ? Buffer.decodeKeyBlockEbc(_buffer.getInt(p
                    + Buffer.KEYBLOCK_LENGTH)) : 0;
            if (ebc2 > ebc) {
                if (getRunCount(i) >= 0) {
                    if (faultAt < 0) {
                        faultAt = i;
                    }
                    return false;
                }
            } else if (ebc > ebc0) {
                if (getRunCount(i) < 0) {
                    if (faultAt < 0) {
                        faultAt = i;
                    }
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Adjusts the Findex array when a KeyBlock is inserted. The update process
     * depends on the ebc of the new KeyBlock relative to its neighbors. Let
     * ebc0 and ebc1 be the ebc values for the keyblocks before and after the
     * newly inserted keyblock, prior to the insertion. (Inserting the new
     * keyblock may cause ebc2 to change.)
     */
    synchronized void insertKeyBlock(final int foundAt, final int previousEbc, final boolean fixupSuccessor) {

        if (!isValid()) {
            return;
        }

        final int start = Buffer.KEY_BLOCK_START;
        final int end = _buffer.getKeyBlockEnd();
        int runIndex = -1;
        int p = foundAt;

        if (p > end) {
            p = end;
        }
        final int insertIndex = (foundAt - start) / Buffer.KEYBLOCK_LENGTH;
        final int lastIndex = (end - start) / Buffer.KEYBLOCK_LENGTH;
        //
        // Insert the associated Findex element.
        //
        System.arraycopy(_findexElements, insertIndex, _findexElements, insertIndex + 1, lastIndex - insertIndex - 1);

        //
        // Adjust all predecessor crossCount and runCount values. While doing
        // this detect the start of a run that the newly inserted key may be
        // part of in runIndex.
        //
        final int insertedEbc = getEbc(insertIndex);

        for (int index = 0; index < insertIndex;) {
            final int runCount = getRunCount(index);
            final int ebc = getEbc(index);

            runIndex = -1;
            if (runCount < 0) {
                //
                // Index of the next element whose ebc is less than or equal to
                // this one.
                //
                final int nextSibling = index - runCount + 1;
                if (insertIndex > nextSibling) {
                    //
                    // We can skip the entire crossCount because the insertion
                    // is after the next sibling.
                    //
                    index = nextSibling;
                } else {
                    if (nextSibling == insertIndex && insertedEbc <= ebc) {
                        //
                        // Can skip the crossCount
                        //
                        index = nextSibling;
                    } else {
                        if (insertedEbc > ebc) {
                            //
                            // Extend the crossCount if we are inserting a
                            // subordinate. Inserting a subordinate cannot
                            // change a sibling's ebc.
                            //
                            putRunCount(index, runCount - 1);
                        }
                        //
                        // Step inside.
                        //
                        index++;
                    }
                }
            } else if (runCount == 0) {
                if (insertedEbc >= ebc) {
                    runIndex = index;
                }
                //
                // Skip this one-element run because by definition it can't have
                // a crossCount.
                //
                index++;
            } else {
                if (insertedEbc >= ebc) {
                    runIndex = index;
                }
                //
                // Skip to the last element of the run because the inserted
                // element is not contiguous with it. (Need to look at the last
                // element because it may have a crossCount.)
                //
                index += runCount;
                //
                // Only handle final element of run specially if it has a
                // crossCount. Otherwise skip it.
                //
                if (getRunCount(index) >= 0) {
                    index++;
                }
            }
        }

        /*
         * The inserted kb is either at the head, tail or in the middle of a
         * run. If runIndex is -1 then the inserted kb is at the head. Otherwise
         * we can measure where it is.
         */
        if (runIndex == -1) {
            // //////
            // HEAD
            // //////
            //
            // The inserted kb is at the head of a run.
            //
            int runCount = getRunCount(insertIndex);
            //
            // If it were less, the key would differ within the first ebc bytes,
            // which is impossible.
            //

            if (insertedEbc > getEbc(insertIndex + 1)) {
                //
                // Can't have a fixup because the successor has a smaller ebc.
                //
                Debug.$assert0.t(!fixupSuccessor);
                putRunCount(insertIndex, 0);
            } else { // insertedEbc == ebc
                if (fixupSuccessor) {
                    //
                    // This is a tricky case because the successor KeyBlock is
                    // changing level.
                    //
                    final int successorEbc = Buffer.decodeKeyBlockEbc(_buffer.getInt(p + Buffer.KEYBLOCK_LENGTH));
                    if (runCount < 0) {
                        runCount = 0;
                    }
                    fixupSuccessor(lastIndex, insertIndex, insertIndex, runCount, previousEbc, successorEbc);
                } else {
                    if (insertIndex + 1 >= lastIndex) {
                        putRunCount(insertIndex, 0);
                    } else if (runCount >= 0) {
                        putRunCount(insertIndex, runCount + 1);
                        putRunCount(insertIndex + 1, 0);
                    } else {
                        putRunCount(insertIndex, 1);
                    }
                }
            }
        } else {
            final int runCount = getRunCount(runIndex);
            final int ebc = getEbc(runIndex);
            Debug.$assert0.t(runCount + runIndex + 1 >= insertIndex);
            if (runCount + runIndex + 1 > insertIndex) {
                // ///////
                // MIDDLE
                // ///////
                // The insertion is in the middle of the run. This means that
                // the successor ebc is the same as the ebc at the head of the
                // run. No fixup is possible.
                //
                if (insertedEbc == ebc) {
                    if (fixupSuccessor) {
                        putRunCount(runIndex, insertIndex - runIndex);
                        //
                        // This is a tricky case because the successor KeyBlock
                        // is changing level.
                        //
                        final int successorEbc = Buffer.decodeKeyBlockEbc(_buffer.getInt(p + Buffer.KEYBLOCK_LENGTH));
                        fixupSuccessor(lastIndex, insertIndex, runIndex, runCount, ebc, successorEbc);
                    } else {
                        putRunCount(runIndex, runCount + 1);
                        putRunCount(insertIndex, 0);
                    }
                } else {
                    Debug.$assert0.t(!fixupSuccessor);
                    if (insertIndex - 1 > runIndex) {
                        putRunCount(runIndex, insertIndex - runIndex - 1);
                    }
                    putRunCount(insertIndex - 1, -1);
                    putRunCount(insertIndex, 0);

                    if (runIndex + runCount > insertIndex) {
                        putRunCount(insertIndex + 1, runCount - (insertIndex - runIndex));
                    }
                }
            } else {
                // ///////
                // END
                // ///////
                //
                // The insertion is at the end of the run.
                //
                if (insertedEbc == ebc) {
                    Debug.$assert0.t(!fixupSuccessor);
                    putRunCount(runIndex, runCount + 1);
                    putRunCount(insertIndex, 0);
                } else {
                    //
                    // If insertedEbc were less, then runIndex would have been
                    // -1.
                    //
                    Debug.$assert0.t(insertedEbc > ebc && !fixupSuccessor && getRunCount(insertIndex - 1) >= 0);
                    putRunCount(insertIndex - 1, -1);
                    putRunCount(insertIndex, 0);
                }
            }
        }
    }

    /**
     * Fixes up the elements surrounding insertion of keyblock that causes the
     * successor ebc to get fixed up.
     * 
     * @param insertIndex
     * @param runIndex
     * @param runCount
     * @param ebc
     * @param successorEbc
     */
    private void fixupSuccessor(final int lastIndex, final int insertIndex, final int runIndex, final int runCount,
            final int ebc, final int successorEbc) {
        if (insertIndex > runIndex) {
            putRunCount(runIndex, insertIndex - runIndex);
        }
        //
        // Test whether fixup is in the middle or at the end of a run
        //
        if (runIndex + runCount > insertIndex) {
            //
            // The fixup happens in the middle of the run, so this is easy. The
            // successor will now run of length 1.
            //
            putRunCount(insertIndex, -1);
            putRunCount(insertIndex + 1, 0);
            final int remainingRunCount = runCount - (insertIndex - runIndex) - 1;
            if (insertIndex + 2 < lastIndex) {
                if (remainingRunCount > 0) {
                    putRunCount(insertIndex + 2, remainingRunCount);
                }
            }
        } else {
            //
            // The kb that's been fixed up is the final member of a run.
            // (Otherwise we should not be here.
            //
            Debug.$assert0.t(runIndex + runCount == insertIndex);
            //
            // The fixup is on the last element of the run.
            //
            putRunCount(insertIndex, runCount - 1);

            final int successorRunCount = insertIndex + 1 < lastIndex ? getRunCount(insertIndex + 1) : 0;
            Debug.$assert0.t(successorRunCount <= 0);
            putRunCount(insertIndex, successorRunCount - 1);

            if (insertIndex + 2 < lastIndex) {
                final int secondSuccessorEbc = getEbc(insertIndex + 2);
                if (successorEbc == secondSuccessorEbc) {
                    //
                    // The fixup has made the successor kb the new start of a
                    // run.
                    //
                    final int secondSuccessorRunCount = getRunCount(insertIndex + 2);
                    if (secondSuccessorRunCount >= 0) {
                        putRunCount(insertIndex + 1, secondSuccessorRunCount + 1);
                        putRunCount(insertIndex + 2, 0);
                    } else {
                        putRunCount(insertIndex + 1, 1);
                    }
                } else {
                    final int newCrossCount = successorEbc < secondSuccessorEbc ? computeCrossCount(successorEbc,
                            insertIndex + 1, lastIndex, insertIndex) : 0;
                    putRunCount(insertIndex + 1, newCrossCount);
                }
            } else {
                Debug.$assert0.t(insertIndex + 1 < lastIndex);
                putRunCount(insertIndex + 1, 0);
            }
        }
    }

    private int computeCrossCount(final int ebc0, final int start, final int end, final int insertIndex) {
        for (int index = start + 1; index < end;) {
            final int ebc = getEbc(index);
            if (ebc <= ebc0) {
                return start - index + 1;
            }
            final int runCount = getRunCount(index);
            if (runCount < 0) {
                index = index + 1 - runCount;
            } else {
                index = index + 1 + runCount;
            }
        }
        return start - end + 1;
    }

    // TODO - unnecessary after debugging
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final Buffer buffer = _buffer;
        if (buffer == null) {
            sb.append("buffer=null");
        } else {
            for (int index = 0; index < buffer.getKeyCount(); index++) {
                final int p = index * 4 + Buffer.KEY_BLOCK_START;
                final int kbData = buffer.getInt(p);
                sb.append(String.format("%4d: runCount=%6d || Buffer p=%5d: ebc=%4d  db=%3d\n", index,
                        getRunCount(index), p, Buffer.decodeKeyBlockEbc(kbData), Buffer.decodeKeyBlockDb(kbData)));
            }
        }
        return sb.toString();
    }
}
