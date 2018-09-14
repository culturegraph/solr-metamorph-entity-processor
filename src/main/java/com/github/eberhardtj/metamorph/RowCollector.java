package com.github.eberhardtj.metamorph;

import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.StreamReceiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowCollector implements StreamReceiver {

    private Map<String,Object> row;

    public RowCollector() {
        this.row = new HashMap<>();
    }

    @Override
    public void closeStream() {
        // Do nothing
    }

    @Override
    public void resetStream() {
        reset();
    }

    private void reset() {
        row = new HashMap<>();
    }

    public Map<String,Object> getRow() {
        return row;
    }

    @Override
    public void startRecord(String s) {
        if (!row.isEmpty())
            reset();
    }

    @Override
    public void endRecord() {
        // Do nothing
    }

    @Override
    public void startEntity(String s) {
        throw new MetafactureException("Entities are not supported!");
    }

    @Override
    public void endEntity() {
        throw new MetafactureException("Entities are not supported!");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void literal(String name, String value) {
        if (row.containsKey(name)) {
            Object existing = row.get(name);
            if (existing instanceof List) {
                List list = (List) existing;
                list.add(value);
            } else {
                List list = new ArrayList();
                list.add(existing);
                list.add(value);
                row.put(name, list);
            }
        } else {
            row.put(name, value);
        }
    }
}
