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

import com.persistit.Exchange.Sequence;

/**
 * @author pbeaman
 */
public abstract class SplitPolicy {
    public final static SplitPolicy LEFT_BIAS = new Left();
    public final static SplitPolicy RIGHT_BIAS = new Right();
    public final static SplitPolicy EVEN_BIAS = new Even();
    public final static SplitPolicy NICE_BIAS = new Nice();
    public final static SplitPolicy PACK_BIAS = new Pack();
    public final static SplitPolicy LEFT90 = new Left90();
    public final static SplitPolicy RIGHT90 = new Right90();
    
    private final static SplitPolicy[] POLICIES = {LEFT_BIAS, RIGHT_BIAS, EVEN_BIAS, NICE_BIAS, PACK_BIAS, LEFT90, RIGHT90};
    
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
     * @return measure of goodness of fit.
     */
    public abstract int splitFit(Buffer buffer, int kbOffset, int insertAt,
            boolean replace, int leftSize, int rightSize, int currentSize,
            int virtualSize, int capacity, int splitInfo, Sequence sequence);

    private static class Left extends SplitPolicy {

        @Override
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
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
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
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
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
            //
            // This implementation maximizes the number of bytes in the left
            // sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return capacity - (int)Math.abs(capacity * .9 - leftSize);
        }

        @Override
        public String toString() {
            return "LEFT90";
        }
    }

    private static class Right90 extends SplitPolicy {
        @Override
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
            //
            // This implementation maximizes the number of bytes
            // moved to the right sibling.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
            return capacity - (int)Math.abs(capacity * .9 - rightSize);
        }

        @Override
        public String toString() {
            return "RIGHT90";
        }
    }

    private static class Even extends SplitPolicy {
        @Override
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
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
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
            //
            // This implementation optimizes toward a 66/34 split - i.e.,
            // biases toward splitting 66% of the records into the
            // left page and 33% into the right page.
            //
            if (leftSize > capacity || rightSize > capacity)
                return 0;
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
        public int splitFit(Buffer buffer, int kbOffset, int insertAt,
                boolean replace, int leftSize, int rightSize, int currentSize,
                int virtualSize, int capacity, int splitInfo, Sequence sequence) {
            switch (sequence) {
            case FORWARD:
                return LEFT90.splitFit(buffer, kbOffset, insertAt, replace,
                        leftSize, rightSize, currentSize, virtualSize,
                        capacity, splitInfo, sequence);
            case REVERSE:
                return RIGHT90.splitFit(buffer, kbOffset, insertAt, replace,
                        leftSize, rightSize, currentSize, virtualSize,
                        capacity, splitInfo, sequence);
            default:
                return EVEN_BIAS.splitFit(buffer, kbOffset, insertAt, replace,
                        leftSize, rightSize, currentSize, virtualSize,
                        capacity, splitInfo, sequence);
            }
        }
        
        @Override
        public String toString() {
            return "PACK";
        }
    }
}
