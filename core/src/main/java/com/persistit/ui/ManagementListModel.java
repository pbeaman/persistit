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

package com.persistit.ui;

import javax.swing.AbstractListModel;

/**
 * A ListModel used to populate lists of volumes and trees.
 * 
 * @author Peter Beaman
 * @version 1.0
 */
public class ManagementListModel extends AbstractListModel {
    private Object[] _infoArray;

    public void setInfoArray(Object[] array) {
        int oldLength = _infoArray == null ? 0 : _infoArray.length;
        int newLength = array == null ? 0 : array.length;
        if (oldLength != newLength && oldLength != 0) {
            fireIntervalRemoved(this, 0, oldLength);
        }
        Object[] oldArray = _infoArray;
        _infoArray = array;
        if (oldLength != newLength) {
            fireIntervalAdded(this, 0, newLength);
        } else if (newLength > 0) {
            int changed = -1;
            for (int index = 0; changed == -1 && index < newLength; index++) {
                if (!array[index].equals(oldArray[index])) {
                    changed = newLength;
                }
            }
            if (changed >= 0) {
                fireContentsChanged(this, changed, newLength);
            }
        }
    }

    public Object[] getInfoArray() {
        return _infoArray;
    }

    @Override
    public Object getElementAt(int index) {
        if (_infoArray != null && index >= 0 && index < _infoArray.length) {
            return _infoArray[index];
        }
        return null;
    }

    @Override
    public int getSize() {
        if (_infoArray == null)
            return 0;
        else
            return _infoArray.length;
    }
}
