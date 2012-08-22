/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
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

    public void setInfoArray(final Object[] array) {
        final int oldLength = _infoArray == null ? 0 : _infoArray.length;
        final int newLength = array == null ? 0 : array.length;
        if (oldLength != newLength && oldLength != 0) {
            fireIntervalRemoved(this, 0, oldLength);
        }
        final Object[] oldArray = _infoArray;
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
    public Object getElementAt(final int index) {
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
