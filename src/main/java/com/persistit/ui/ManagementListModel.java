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
