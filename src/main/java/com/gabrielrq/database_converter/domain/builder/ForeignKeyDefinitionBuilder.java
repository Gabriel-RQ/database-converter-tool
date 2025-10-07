package com.gabrielrq.database_converter.domain.builder;

import com.gabrielrq.database_converter.domain.ForeignKeyDefinition;

import java.util.ArrayList;
import java.util.List;

public class ForeignKeyDefinitionBuilder {
    private final String name;
    private final String referencedTable;
    private final List<String> localColumns = new ArrayList<>();
    private final List<String> referencedColumns = new ArrayList<>();

    public ForeignKeyDefinitionBuilder(String name, String referencedTable) {
        this.name = name;
        this.referencedTable = referencedTable;
    }

    public ForeignKeyDefinitionBuilder addColumnPair(String local, String referenced) {
        this.localColumns.add(local);
        this.referencedColumns.add(referenced);
        return this;
    }

    public ForeignKeyDefinition build() {
        return new ForeignKeyDefinition(name, referencedTable, localColumns, referencedColumns);
    }
}
