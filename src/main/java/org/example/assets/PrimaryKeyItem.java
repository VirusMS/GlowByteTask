package org.example.assets;

import lombok.Data;

import java.util.Map;


@Data
public class PrimaryKeyItem {

    private final String tableName;
    private final Map<String, String> primaryKeys;

}
