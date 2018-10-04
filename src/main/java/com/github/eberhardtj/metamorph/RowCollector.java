package com.github.eberhardtj.metamorph;

import org.metafacture.framework.MetafactureException;
import org.metafacture.framework.StreamReceiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowCollector implements StreamReceiver {

    private Map<String,Object> row;
    private String currentEntity;

    public RowCollector() {
        this.row = new HashMap<>();
        this.currentEntity = "";
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
        currentEntity = "";
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
        currentEntity = s;
    }

    @Override
    public void endEntity() {
        currentEntity = "";
    }

    private String camelCase(String prefix, String name) {
        return prefix.toLowerCase() + name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void literal(String name, String value) {

        String literalName = currentEntity.isEmpty() ? name : camelCase(currentEntity, name);

        if (row.containsKey(literalName)) {
            Object existing = row.get(literalName);
            if (existing instanceof List) {
                List list = (List) existing;
                list.add(value);
            } else {
                List list = new ArrayList();
                list.add(existing);
                list.add(value);
                row.put(literalName, list);
            }
        } else {
            row.put(literalName, value);
        }
    }
}
