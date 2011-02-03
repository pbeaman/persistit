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

    public Object getElementAt(int index) {
        if (_infoArray != null && index >= 0 && index < _infoArray.length) {
            return _infoArray[index];
        }
        return null;
    }

    public int getSize() {
        if (_infoArray == null)
            return 0;
        else
            return _infoArray.length;
    }
}
