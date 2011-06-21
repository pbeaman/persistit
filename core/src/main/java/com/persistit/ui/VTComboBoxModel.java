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

import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractListModel;
import javax.swing.ComboBoxModel;

import com.persistit.Management;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;

class VTComboBoxModel extends AbstractListModel implements ComboBoxModel {

    private VTComboBoxModel _parent;

    private List _cachedList = new ArrayList();
    private Object _selectedItem;
    private long _validTime;
    private Management _management;

    public VTComboBoxModel(VTComboBoxModel vtcmb, Management management) {
        _parent = vtcmb;
        _management = management;
    }

    @Override
    public Object getElementAt(int index) {
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
    public void setSelectedItem(Object item) {
        _selectedItem = item;
    }

    @Override
    public Object getSelectedItem() {
        return _selectedItem;
    }

    private void populateList() {
        long now = System.currentTimeMillis();
        if (now - _validTime < 1000)
            return;
        _validTime = now;
        _cachedList.clear();
        try {
            if (_parent == null) {
                VolumeInfo[] volumes = _management.getVolumeInfoArray();
                for (int index = 0; index < volumes.length; index++) {
                    _cachedList.add(volumes[index].getPath());
                }
            } else {
                String volumeName = (String) _parent.getSelectedItem();
                if (volumeName == null)
                    return;
                TreeInfo[] trees = _management.getTreeInfoArray(volumeName);
                for (int index = 0; index < trees.length; index++) {
                    _cachedList.add(trees[index].getName());
                }
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private void handleException(Exception e) {
        e.printStackTrace();
    }
}