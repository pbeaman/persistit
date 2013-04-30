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

import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import com.persistit.Management;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;

class VTComboBoxModel extends AbstractListModel implements ComboBoxModel {

    private final VTComboBoxModel _parent;

    private final List _cachedList = new ArrayList();
    private Object _selectedItem;
    private long _validTime;
    private final Management _management;

    public VTComboBoxModel(final VTComboBoxModel vtcmb, final Management management) {
        _parent = vtcmb;
        _management = management;
    }

    @Override
    public Object getElementAt(final int index) {
        populateList();
        if (index >= 0 && index < _cachedList.size()) {
            return _cachedList.get(index);
        }
        return null;
    }

    @Override
    public int getSize() {
        populateList();
        return _cachedList.size();
    }

    @Override
    public void setSelectedItem(final Object item) {
        _selectedItem = item;
    }

    @Override
    public Object getSelectedItem() {
        return _selectedItem;
    }

    private void populateList() {
        final long now = System.currentTimeMillis();
        if (now - _validTime < 1000)
            return;
        _validTime = now;
        _cachedList.clear();
        try {
            if (_parent == null) {
                final VolumeInfo[] volumes = _management.getVolumeInfoArray();
                for (int index = 0; index < volumes.length; index++) {
                    _cachedList.add(volumes[index].getName());
                }
            } else {
                final String volumeName = (String) _parent.getSelectedItem();
                if (volumeName == null)
                    return;
                final TreeInfo[] trees = _management.getTreeInfoArray(volumeName);
                for (int index = 0; index < trees.length; index++) {
                    _cachedList.add(trees[index].getName());
                }
            }
        } catch (final Exception e) {
            handleException(e);
        }
    }

    private void handleException(final Exception e) {
        e.printStackTrace();
    }
}
