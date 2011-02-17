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

/**
 * Policy for determining the balance between left and right pages when
 * rebalancing page content due to deletion.
 * 
 * @version 1.0
 */
public class JoinPolicy {
    /**
     * A policy that maximizes the number of records in the left sibling page,
     * and minimizes the number of records in the right sibling page.
     */
    public final static JoinPolicy LEFT_BIAS = new JoinPolicy(-1, "LEFT BIAS");
    /**
     * A policy that minimizes the number of records in the left sibling page,
     * and maximizes the number of records in the right sibling page.
     */
    public final static JoinPolicy RIGHT_BIAS = new JoinPolicy(1, "RIGHT BIAS");
    /**
     * A policy that attempts to evenly balance the number of records in the
     * left and right sibling pages.
     */
    public final static JoinPolicy EVEN_BIAS = new JoinPolicy(0, "EVEN BIAS");

    String _name;
    int _bias;

    protected JoinPolicy(int bias, String name) {
        _bias = bias;
        _name = name;
    }

    /**
     * Determines the quality of fit for a specified candidate join location
     * within a page. Returns 0 if no join is possible, or a positive integer if
     * a join is possible. The caller will call joinFit to evaluate several
     * possible join locations, and will choose the one yielding the largest
     * value of this method.
     * 
     * @param leftBuffer
     *            The left <tt>Buffer></tt>
     * @param rightBuffer
     *            The right <tt>Buffer</tt>
     * @param kbOffset
     *            The key block proposed as the new split point
     * @param foundAt1
     *            First key being deleted from left page
     * @param foundAt2
     *            First key not being deleted from right page
     * @param virtualSize
     *            The total size of both pages
     * @param leftSize
     *            Size the left page would have if split here
     * @param rightSize
     *            Size the right page would have if split here
     * @param capacity
     *            The total space available in a page (less overhead)
     * @return A measure of "goodness of fit" This method should return the
     *         largest such measure for the best split point
     */

    public int rebalanceFit(Buffer leftBuffer, Buffer rightBuffer,
            int kbOffset, int foundAt1, int foundAt2, int virtualSize,
            int leftSize, int rightSize, int capacity) {
        //
        // This implementation minimizes the difference -- i.e., attempts
        // to join the page into equally sized siblings.
        //
        if (leftSize > capacity || rightSize > capacity)
            return 0;
        int fitness;

        switch (_bias) {
        case -1:
            fitness = leftSize;
            break;

        case 0:
            int difference = rightSize - leftSize;
            if (difference < 0)
                difference = -difference;
            fitness = capacity - difference;
            break;

        case 1:
            fitness = rightSize;
            break;

        default:
            throw new IllegalArgumentException("Invalid bias");
        }
        return fitness;
    }

    /**
     * Determines whether two pages will be permitted to be rejoined during a
     * delete operation.
     * 
     * @param buffer
     * @param virtualSize
     * @return <tt>true</tt> if the buffer will accept content of the specified
     *         size
     */
    protected boolean acceptJoin(Buffer buffer, int virtualSize) {
        return virtualSize < buffer.getBufferSize();
    }
    
    /**
     * @return name of the policy
     */
    public String getName() {
        return _name;
    }
    
}
