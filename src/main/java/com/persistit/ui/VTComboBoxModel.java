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
                    _cachedList.add(volumes[index].getPath());
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
