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