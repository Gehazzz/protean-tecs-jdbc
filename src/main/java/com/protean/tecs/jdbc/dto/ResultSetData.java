package com.protean.tecs.jdbc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResultSetData {
    private int columnCount;
    private String schemaName;
    private String tableName;
    private String interceptionPoint;
    private List<RowData> rowsData;
}
