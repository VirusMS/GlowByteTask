package org.example.assets;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class TableListItem {

    private final String tableName;
    private final List<String> primaryKeys;

    public TableListItem(String tableName, String primaryKeys) {
        List<String> keysList = new ArrayList<>();

        String[] keys = primaryKeys.split(",");
        for (String key : keys) {
            keysList.add(key.trim()); // Based on the task, ',' is a delimiter. However, there may still be whitespaces, so we have to trim these.
        }

        this.primaryKeys = new ArrayList<>(keysList);
        this.tableName = tableName;
    }

}
