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

package com.persistit;

public class FastIndex {
    
    /**
     * Indicates whether the _findexElements array is valid
     */
    private boolean _isFindexValid;

    /**
     * The Findex array. One element per keyblock holds the crossCount,
     * runCount, ebc and db from the keyblock.
     */
    private int[] _findexElements;
    
    /**
     * The buffer this fast index is associated with.
     */
    private Buffer _buffer;
    
    private final static int FINDEX_DB_MASK = 0x000000FF;
    private final static int FINDEX_EBC_MASK = 0x000FFF00;
    private final static int FINDEX_EBC_SHIFT = 8;
    private final static int FINDEX_RUNCOUNT_MASK = 0xFFF00000;
    private final static int FINDEX_RUNCOUNT_SHIFT = 20;
    
    public FastIndex(int elementSize, Buffer associatedBuffer) {
        _findexElements = new int[elementSize];
        assert associatedBuffer != null;
        _buffer = associatedBuffer;
        _isFindexValid = false;
    }
    
    public int size() {
        return _findexElements.length;
    }

    public void putDescriminatorByte(int index, int db) {
        assert index < _findexElements.length;
        _findexElements[index] = (db & FINDEX_DB_MASK)
                | (_findexElements[index] & ~FINDEX_DB_MASK);
    }

    public void putRunCount(int index, int runCount) {
        assert index < _findexElements.length;
        _findexElements[index] = (runCount << FINDEX_RUNCOUNT_SHIFT)
                | (_findexElements[index] & ~FINDEX_RUNCOUNT_MASK);
    }

    public void putEbc(int index, int ebc) {
        assert index < _findexElements.length;
        _findexElements[index] = ((ebc << FINDEX_EBC_SHIFT) & FINDEX_EBC_MASK)
                | (_findexElements[index] & ~FINDEX_EBC_MASK);
    }

    public void putRunCountAndEbc(int index, int runCount, int ebc) {
        assert index < _findexElements.length;
        _findexElements[index] = ((ebc << FINDEX_EBC_SHIFT) & FINDEX_EBC_MASK)
                | (runCount << FINDEX_RUNCOUNT_SHIFT)
                | (_findexElements[index] & FINDEX_DB_MASK);
    }

    public void putZero(int index) {
        assert index < _findexElements.length;
        _findexElements[index] = _findexElements[index] & FINDEX_DB_MASK;
    }
    
    public int getRunCount(int index) {
        assert index < _findexElements.length;
        return _findexElements[index] >> FINDEX_RUNCOUNT_SHIFT;
    }

    public int getEbc(int index) {
        assert index < _findexElements.length;
        int ebc = (_findexElements[index] & FINDEX_EBC_MASK) >> FINDEX_EBC_SHIFT;
        if (ebc > 2047) {
            return ebc - 4096;
        } else {
            return ebc;
        }
    }
    
    public int getDescriminatorByte(int index) {
        assert index < _findexElements.length;
        return _findexElements[index] & FINDEX_DB_MASK;
    }
    
    public boolean isValid() {
        return _isFindexValid;
    }
    
    public void validate() {
        _isFindexValid = true;
        if (! verify()) {
            _isFindexValid = false;
        }
    }
    
    public void invalidate() {
        _isFindexValid = false;
    }
    
    public synchronized void recompute() {
        if (_isFindexValid) {
            if (Debug.ENABLED) {
                verify();
            }
        } else if (_buffer.isDataPage() || _buffer.isIndexPage()) {
            int start = _buffer.getKeyBlockStart();
            int end = _buffer.getKeyBlockEnd();

            int ebc0 = 0;
            int runCountFixupIndex = 0;
            int crossCountFixupIndex = -1;
            int lastIndex = (end - start) / Buffer.KEYBLOCK_LENGTH;

            for (int i = 0, p = start; i <= lastIndex; i++, p += Buffer.KEYBLOCK_LENGTH) {
                int ebc;
                if (i < lastIndex || i == 0) {
                    int kbData = _buffer.getInt(p);
                    ebc = Buffer.decodeKeyBlockEbc(kbData);
                    putEbc(i, ebc);
                } else {
                    ebc = -1;
                    putEbc(i, 0);
                }

                if (ebc != ebc0) {
                    int runCount = i - runCountFixupIndex - 1;
                    putRunCount(runCountFixupIndex, runCount);
                    runCountFixupIndex = i;

                    if (ebc > ebc0) {
                        /*
                         * If not true then the ebc for the very first
                         * KeyBlock is non-zero, which is wrong.
                         */
                        assert i > 0;
                        putRunCountAndEbc(i - 1, crossCountFixupIndex, ebc0);
                        crossCountFixupIndex = i - 1;
                    } else { /* ebc < ebc0 */
                        /*
                         * Now we need to walk back through the linked list
                         * of findex array elements that need to have their
                         * crossCount field updated.
                         */
                        for (int j = crossCountFixupIndex; j != -1;) {
                            int ccFixupEbc = getEbc(j);

                            if (ebc <= ccFixupEbc) {
                                int crossCount = -(i - j - 1);

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
                    _findexElements[i] = 0;
                }
                int kbData = _buffer.getInt(p);
                putDescriminatorByte(i, _buffer.decodeKeyBlockDb(kbData));
            }
            if (Debug.ENABLED) {
                verify();
            }
            _isFindexValid = true;
        } else {
            _isFindexValid = false;
        }
    }
    
    /**
     * @return true iff the index is valid; false otherwise
     */
    public boolean verify() {
        if (!isValid()) {
            return false;
        }
        int start = _buffer.getKeyBlockStart();
        int end = _buffer.getKeyBlockEnd();
        /*
         * This is the index of a Findex array entry that needs to be fixed
         * up with its true count.
         */

        int ebc0;
        int ebc = -1;
        int ebc2 = Buffer.decodeKeyBlockEbc(_buffer.getInt(start));
        int faultAt = -1;

        for (int i = 0, p = start; p < end; i++, p += Buffer.KEYBLOCK_LENGTH) {
            ebc0 = ebc;
            ebc = ebc2;
            ebc2 = p + Buffer.KEYBLOCK_LENGTH < end ? Buffer
                    .decodeKeyBlockEbc(_buffer.getInt(p
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
    public synchronized void insertKeyBlock(int foundAt, 
                                            int insertedEbc,
                                            boolean fixupSuccessor) {
        if (_findexElements == null) {
            return;
        }
        if (!_isFindexValid) {
            recompute();
            return;
        }
        assert _findexElements != null;
        int p = foundAt & Buffer.P_MASK;
        int start = _buffer.getKeyBlockStart();
        int end = _buffer.getKeyBlockEnd();
        int runIndex = -1;

        if (p > end) {
            p = end;
        }
        int insertIndex = (foundAt - start) / Buffer.KEYBLOCK_LENGTH;
        int lastIndex = (end - start) / Buffer.KEYBLOCK_LENGTH;
        /*
         * Insert the associated Findex element.
         */
        System.arraycopy(_findexElements, 
                         insertIndex, 
                         _findexElements,
                         insertIndex + 1, 
                         lastIndex - insertIndex - 1);

        int kbData = _buffer.getInt(p);
        putDescriminatorByte(insertIndex, Buffer.decodeKeyBlockDb(kbData));
        /*
         * Adjust all predecessor crossCount and runCount values
         */
        for (int index = 0; index < insertIndex;) {
            int runCount = getRunCount(index);
            int ebc = getEbc(index);

            runIndex = -1;
            if (runCount < 0) {
                /*
                 * Index of the next element whose ebc is less than or equal to
                 * this one.
                 */
                int nextSibling = index - runCount + 1;
                if (insertIndex > nextSibling) {
                    /*
                     * We can skip the entire crossCount because the insertion
                     * is after the next sibling.
                     */
                    index = nextSibling;
                } else {
                    if (nextSibling == insertIndex && insertedEbc <= ebc) {
                        /* Can skip the crossCount */
                        index = nextSibling;
                    } else {
                        if (insertedEbc > ebc) {
                            /*
                             * Extend the crossCount if we are inserting a
                             * subordinate. Inserting a subordinate cannot
                             * change a sibling's ebc.
                             */
                            putRunCount(index, runCount - 1);
                        }
                        /* Step inside. */
                        index++;
                    }
                }
            } else if (runCount == 0) {
                if (insertedEbc >= ebc)
                    runIndex = index;
                /*
                 * Skip this one-element run because by definition it can't
                 * have a crossCount.
                 */
                index++;
            } else {
                if (insertedEbc >= ebc) {
                    runIndex = index;
                }
                /*
                 * Skip to the last element of the run because the
                 * inserted element is not contiguous with it.
                 * (Need to look at the last element because it may have
                 * a crossCount.)
                 */
                index += runCount;
                /*
                 * Only handle final element of run specially if it has
                 * a crossCount. Otherwise skip it.
                 */
                if (getRunCount(index) >= 0) {
                    index++;
                }
            }
        }

        /*
         * The inserted kb is either at the head, tail or in the middle of
         * a run. If runIndex is -1 then the inserted kb is at the head.
         * Otherwise we can measure where it is.
         */
        if (runIndex == -1) {
            // //////
            // HEAD
            // //////
            //
            // The inserted kb is at the head of a run.
            //
            int ebc = getEbc(insertIndex);
            int runCount = getRunCount(insertIndex);
            /*
             * If it were less, the key would differ within the first ebc bytes,
             * which is impossible.
             */

            assert insertedEbc >= ebc;

            if (insertedEbc > ebc) {
                /* Can't have a fixup because the sucessor has a smaller ebc. */
                assert ! fixupSuccessor;
                putRunCountAndEbc(insertIndex, 0, insertedEbc);
            } else { /* insertedEbc == ebc */
                if (fixupSuccessor) {
                    /*
                     * This is a tricky case because the successor KeyBlock
                     * is changing level.
                     */
                    int successorEbc = Buffer.decodeKeyBlockEbc(_buffer.getInt(p + Buffer.KEYBLOCK_LENGTH));
                    if (runCount < 0) {
                        runCount = 0;
                    }
                    fixupSuccessor(lastIndex, 
                                   insertIndex, 
                                   insertIndex,
                                   runCount, 
                                   ebc, 
                                   successorEbc);
                } else {
                    if (insertIndex + 1 >= lastIndex) {
                        putRunCountAndEbc(insertIndex, 0, ebc);
                    } else if (runCount >= 0) {
                        putRunCountAndEbc(insertIndex, runCount + 1, ebc);
                        putZero(insertIndex + 1);
                    } else {
                        putRunCountAndEbc(insertIndex, 1, ebc);
                    }
                }
            }
        } else {
            int runCount = getRunCount(runIndex);
            int ebc = getEbc(runIndex);
            assert runCount + runIndex + 1 >= insertIndex;
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
                        /*
                         * This is a tricky case because the successor KeyBlock
                         * is changing level.
                         */
                        int successorEbc = Buffer.decodeKeyBlockEbc(_buffer.getInt(p
                                + Buffer.KEYBLOCK_LENGTH));

                        fixupSuccessor(lastIndex, 
                                       insertIndex, 
                                       runIndex,
                                       runCount, 
                                       ebc, 
                                       successorEbc);
                    } else {
                        putRunCount(runIndex, runCount + 1);
                        putZero(insertIndex);
                    }
                } else {
                    assert ! fixupSuccessor;
                    if (insertIndex - 1 > runIndex) {
                        putRunCount(runIndex, insertIndex - runIndex - 1);
                    }
                    putRunCountAndEbc(insertIndex - 1, -1, ebc);
                    putRunCountAndEbc(insertIndex, 0, insertedEbc);

                    if (runIndex + runCount > insertIndex) {
                        putRunCountAndEbc(insertIndex + 1, 
                                          runCount - (insertIndex - runIndex),
                                          ebc);
                    } else {
                        putEbc(insertIndex + 1, ebc);
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
                    assert ! fixupSuccessor;
                    putRunCount(runIndex, runCount + 1);
                    putZero(insertIndex);
                } else {
                    /*
                     * If insertedEbc were less, then runIndex would have
                     * been -1.
                     */
                    assert insertedEbc > ebc;
                    assert ! fixupSuccessor;
                    assert getRunCount(insertIndex - 1) >= 0;
                    putRunCountAndEbc(insertIndex - 1, -1, ebc);
                    putRunCountAndEbc(insertIndex, 0, insertedEbc);
                }
            }
        }
        if (Debug.ENABLED) {
            verify();
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
    private void fixupSuccessor(int lastIndex, 
                                int insertIndex,
                                int runIndex, 
                                int runCount, 
                                int ebc, 
                                int successorEbc) {
        int p = _buffer.getKeyBlockStart() + (insertIndex + 1) * Buffer.KEYBLOCK_LENGTH;
        int kbData = _buffer.getInt(p);
        putDescriminatorByte(insertIndex + 1, Buffer.decodeKeyBlockDb(kbData));

        if (insertIndex > runIndex) {
            putRunCount(runIndex, insertIndex - runIndex);
        }
        /* Test whether fixup is in the middle or at the end of a run */
        if (runIndex + runCount > insertIndex) {
            /*
             * The fixup happens in the middle of the run, so this is
             * easy. The successor will now run of length 1.
             */
            putRunCountAndEbc(insertIndex, -1, ebc);
            putRunCountAndEbc(insertIndex + 1, 0, successorEbc);
            int remainingRunCount = runCount - (insertIndex - runIndex) - 1;
            if (insertIndex + 2 < lastIndex) {
                if (remainingRunCount > 0) {
                    putRunCountAndEbc(insertIndex + 2, remainingRunCount, ebc);
                } else {
                    putEbc(insertIndex + 2, ebc);
                }
            }
        } else {
            /*
             * The kb that's been fixed up is the final member of a run.
             * (Otherwise we should not be here.
             */
            assert runIndex + runCount == insertIndex;
            /*
             * The fixup is on the last element of the run.
             */
            putRunCount(insertIndex, runCount - 1);

            int successorRunCount = insertIndex + 1 < lastIndex ? getRunCount(insertIndex + 1) : 0;
            assert successorRunCount <= 0;
            putRunCountAndEbc(insertIndex, successorRunCount - 1, ebc);

            if (insertIndex + 2 < lastIndex) {
                int secondSuccessorEbc = getEbc(insertIndex + 2);
                if (successorEbc == secondSuccessorEbc) {
                    /*
                     * The fixup has made the successor kb the new
                     * start of a run.
                     */
                    int secondSuccessorRunCount = getRunCount(insertIndex + 2);
                    if (secondSuccessorRunCount >= 0) {
                        putRunCountAndEbc(insertIndex + 1,
                                          secondSuccessorRunCount + 1, 
                                          successorEbc);
                        putZero(insertIndex + 2);
                    } else {
                        putRunCountAndEbc(insertIndex + 1, 1, successorEbc);
                    }
                } else {
                    putEbc(insertIndex + 1, successorEbc);
                    int newCrossCount = successorEbc < secondSuccessorEbc ? computeCrossCount(
                            successorEbc, insertIndex + 1, lastIndex) : 0;
                    putRunCount(insertIndex + 1, newCrossCount);
                }
            } else {
                assert insertIndex + 1 < lastIndex;
                putRunCountAndEbc(insertIndex + 1, 0, successorEbc);
            }
        }
    }

    private int computeCrossCount(int ebc0, int start, int end) {
        for (int index = start + 1; index < end;) {
            int ebc = getEbc(index);
            if (ebc <= ebc0) {
                return start - index + 1;
            }
            int runCount = getRunCount(index);
            if (runCount < 0) {
                index = index + 1 - runCount;
            } else {
                index = index + 1 + runCount;
            }
        }
        return start - end + 1;

    }
}
