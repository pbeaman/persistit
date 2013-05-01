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

package com.persistit.policy;

import com.persistit.Buffer;
import com.persistit.Exchange.Sequence;

/**
 * Policies for determining the balance between left and right pages when
 * insertion causes a page to split.
 * 
 * @author peter
 */
public abstract class SplitPolicy {
    /**
     * Allocate as many records as possible to the left page
     */
    public final static SplitPolicy LEFT_BIAS = new Left();
    /**
     * Allocate as many records as possible to the right page
     */
    public final static SplitPolicy RIGHT_BIAS = new Right();
    /**
     * Allocate as records evenly between left and right pages
     */
    public final static SplitPolicy EVEN_BIAS = new Even();
    /**
     * Allocate about 2/3 of records to the left and 1/3 to the right page
     */
    public final static SplitPolicy NICE_BIAS = new Nice();
    /**
     * Equivalent to {@link #LEFT90_BIAS} or {{@link #RIGHT90_BIAS} when records
     * are being inserted in sequential key order, otherwise equivalent to
     * {@link #NICE_BIAS}
     */
    public final static SplitPolicy PACK_BIAS = new Pack();

    /**
     * Allocate all records to the left page until it is about 90% full, then
     * allocate remaining records to the right page. Like LEFT_BIAS except this
     * policy attempts to leave the left page 10% empty.
     */
    public final static SplitPolicy LEFT90_BIAS = new Left90();
    /**
     * Allocate all records to the right page until it is about 90% full, then
     * allocate remaining records to the right page. Like RIGHT_BIAS except this
     * policy attempts to leave the right page 10% empty.
     */
    public final static SplitPolicy RIGHT90_BIAS = new Right90();

    final static int KEYBLOCK_LENGTH = 4;

    private final static SplitPolicy[] POLICIES = { LEFT_BIAS, RIGHT_BIAS, EVEN_BIAS, NICE_BIAS, PACK_BIAS,
            LEFT90_BIAS, RIGHT90_BIAS };

    private final static float PACK_SHOULDER = 0.9f;

    public static SplitPolicy forName(final String name) {
        for (final SplitPolicy policy : POLICIES) {
            if (policy.toString().equalsIgnoreCase(name)) {
                return policy;
            }
        }
        throw new IllegalArgumentException("No such SplitPolicy " + name);
    }

    /**
     * Determines the quality of fit for a specified candidate split location
     * within a page. Returns 0 if no split is possible, or a positive integer
     * if a split is possible. The caller will call splitFit to evaluate several
     * possible split locations, and will choose the one yielding the largest
     * value of this method.
     * 
     * @param buffer
     *            the buffer being split.
     * @param kbOffset
     *            offset of the proposed keyblock that will become the first key
     *            of the right sibling page.
     * @param insertAt
     *            offset of the keyblock where the record insertion will occur.
     * @param replace
     *            <i>true</i> if the record at kbOffset is being replaced
     * @param leftSize
     *            size of the left sibling page that would result if the split
     *            were done at this candidate location.
     * @param rightSize
     *            size of the right sibling page that would result if the split
     *            were done at this candidate location.
     * @param currentSize
     *            size of the page prior to insertion.
     * @param virtualSize
     *            size of the page that would result where the insertion to
     *            occur without splitting the page. (This is usually larger than
     *            the actual capacity of the buffer, which is why it needs to be
     *            split.)
     * @param capacity
     *            Actual available bytes in a page.
     * @param splitBest
     *            the previous best-fit result from this method, or 0 if there
     *            is no candidate split location yet.
     * @param sequence
     *            current sequential insert state
     * @return measure of goodness of fit.
     */
    public abstract int splitFit(Buffer buffer, int kbOffset, int insertAt, boolean replace, int leftSize,
            int rightSize, int currentSize, int virtualSize, int capacity, int splitBest, Sequence sequence);

    private static class Left extends SplitPolicy {

        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation maximizes the number of bytes in the left
            // sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return leftSize;
        }

        @Override
        public String toString() {
            return "LEFT";
        }
    }

    private static class Right extends SplitPolicy {
        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation maximizes the number of bytes
            // moved to the right sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return rightSize;
        }

        @Override
        public String toString() {
            return "RIGHT";
        }
    }

    private static class Left90 extends SplitPolicy {

        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation maximizes the number of bytes in the left
            // sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return capacity - (int) Math.abs(capacity * PACK_SHOULDER - leftSize);
        }

        @Override
        public String toString() {
            return "LEFT90";
        }
    }

    private static class Right90 extends SplitPolicy {
        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation maximizes the number of bytes
            // moved to the right sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return capacity - (int) Math.abs(capacity * PACK_SHOULDER - rightSize);
        }

        @Override
        public String toString() {
            return "RIGHT90";
        }
    }

    private static class Even extends SplitPolicy {
        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation minimizes the difference -- i.e., attempts
            // to split the page into equally sized siblings.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            int difference = rightSize - leftSize;
            if (difference < 0) {
                difference = -difference;
            }
            return capacity - difference;
        }

        @Override
        public String toString() {
            return "EVEN";
        }
    }

    private static class Nice extends SplitPolicy {
        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {
            //
            // This implementation optimizes toward a 66/34 split - i.e.,
            // biases toward splitting 66% of the records into the
            // left page and 33% into the right page.
            //
            if (leftSize > capacity || rightSize > capacity) {
                return 0;
            }
            int difference = 2 * rightSize - leftSize;
            if (difference < 0) {
                difference = -difference;
            }
            return capacity * 2 - difference;
        }

        @Override
        public String toString() {
            return "NICE";
        }
    }

    private static class Pack extends SplitPolicy {
        @Override
        public int splitFit(final Buffer buffer, final int kbOffset, final int insertAt, final boolean replace,
                final int leftSize, final int rightSize, final int currentSize, final int virtualSize,
                final int capacity, final int splitInfo, final Sequence sequence) {

            //
            // This policy is identical to Nice except when the split is caused
            // by a sequential insertion. In the sequential case, this method
            // attempts to bias the split toward the insertion point. For
            // forward sequential inserts, this means the preferred location to
            // split the page is immediately before the next key in the page
            // after the newly inserted one. This lets subsequent sequential
            // insertions fill up the current page without having to split it
            // again right away.
            //
            // To avoid over-packing pages, we add shoulders to this behavior so
            // that if neither portion of the page will be more than XX% full
            // (where optimal XX is TBD).
            //
            if (leftSize > capacity || rightSize > capacity) {
                return 0;
            }

            if (!replace) {
                if (sequence == Sequence.FORWARD) {
                    final int shoulder = (int) (capacity * PACK_SHOULDER);
                    final int keyOffsetCost = Math.abs(insertAt - kbOffset);
                    if (leftSize < shoulder && rightSize < shoulder) {
                        return capacity * 2 - keyOffsetCost;
                    } // otherwise revert to NICE
                } else if (sequence == Sequence.REVERSE) {
                    final int shoulder = (int) (capacity * PACK_SHOULDER);
                    final int keyOffsetCost = Math.abs(insertAt - kbOffset + KEYBLOCK_LENGTH);
                    if (leftSize < shoulder && rightSize < shoulder) {
                        return capacity * 2 - keyOffsetCost;
                    } // otherwise revert to NICE
                }
            }

            return NICE_BIAS.splitFit(buffer, kbOffset, insertAt, replace, leftSize, rightSize, currentSize,
                    virtualSize, capacity, splitInfo, sequence);
        }

        @Override
        public String toString() {
            return "PACK";
        }
    }
}
