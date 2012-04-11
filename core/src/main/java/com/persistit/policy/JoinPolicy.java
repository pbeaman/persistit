/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.policy;

import com.persistit.Buffer;

/**
 * Policies for determining the balance between left and right pages when
 * rebalancing page content due to deletion.
 * 
 * @version 1.0
 */
public class JoinPolicy {
    /**
     * Maximize the number of records in the left page,
     * and minimize the number of records in the right page.
     */
    public final static JoinPolicy LEFT_BIAS = new JoinPolicy(-1, "LEFT");
    /**
     * Minimize the number of records in the left page,
     * and maximize the number of records in the right page.
     */
    public final static JoinPolicy RIGHT_BIAS = new JoinPolicy(1, "RIGHT");
    /**
     * Balance the allocation of spaces evenly between
     * left and right pages.
     */
    public final static JoinPolicy EVEN_BIAS = new JoinPolicy(0, "EVEN");

    private final static JoinPolicy[] POLICIES = { LEFT_BIAS, RIGHT_BIAS, EVEN_BIAS };

    public static JoinPolicy forName(final String name) {
        for (final JoinPolicy policy : POLICIES) {
            if (policy.toString().equalsIgnoreCase(name)) {
                return policy;
            }
        }
        throw new IllegalArgumentException("No such SplitPolicy " + name);
    }

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
     *            The left <code>Buffer></code>
     * @param rightBuffer
     *            The right <code>Buffer</code>
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

    public int rebalanceFit(Buffer leftBuffer, Buffer rightBuffer, int kbOffset, int foundAt1, int foundAt2,
            int virtualSize, int leftSize, int rightSize, int capacity) {
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
     * @return <code>true</code> if the buffer will accept content of the
     *         specified size
     */
    public boolean acceptJoin(Buffer buffer, int virtualSize) {
        return virtualSize < buffer.getBufferSize();
    }

    /**
     * @return name of the policy
     */
    public String getName() {
        return _name;
    }

}
