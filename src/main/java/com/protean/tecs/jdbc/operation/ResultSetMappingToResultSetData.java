package com.protean.tecs.jdbc.operation;

import com.protean.tecs.jdbc.dto.ColumnData;
import com.protean.tecs.jdbc.dto.ResultSetData;
import com.protean.tecs.jdbc.dto.RowData;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResultSetMappingToResultSetData implements ResultSetMapping<ResultSetData> {
    @Override
    public ResultSetData resultSetToDto(ResultSet resultSet) throws SQLException {
        return resultSetToDto(resultSet, Thread.currentThread().getStackTrace()[2].getMethodName());
    }

    @Override
    public ResultSetData resultSetToDto(ResultSet resultSet, String interceptionPoint) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<RowData> rowsData = new ArrayList<>();
        while (resultSet.next()) {
            List<ColumnData> row = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                ColumnData columnData = ColumnData.builder()
                        .index(i)
                        .value(resultSet.getString(i))
                        .columnName(metaData.getColumnName(i))
                        .columnLabel(metaData.getColumnLabel(i))
                        .isAutoIncrement(metaData.isAutoIncrement(i))
                        .catalogName(metaData.getCatalogName(i))
                        .columnClassName(metaData.getColumnClassName(i))
                        .columnDisplaySize(metaData.getColumnDisplaySize(i))
                        .columnType(metaData.getColumnType(i))
                        .columnTypeName(metaData.getColumnTypeName(i))
                        .scale(metaData.getScale(i))
                        .precision(metaData.getPrecision(i))
                        .build();
                row.add(columnData);
            }
            Map<String, ColumnData> columns = row.stream().collect(Collectors.toMap(ColumnData::getColumnName, Function.identity()));
            /*String columnName = columns.get("COLUMN_NAME").getValue();
            String tableName = columns.get("TABLE_NAME").getValue();*/

            RowData rowData = RowData.builder()
                    .columns(columns)
                    .build();
            rowsData.add(rowData);
        }

/*        Map<String, String> columns = rowsData.stream()
                .collect(Collectors.toMap(rowData -> rowData.getColumns().get("COLUMN_NAME").getValue(), rowData -> rowData.getColumns().get("TABLE_NAME").getValue()));*/

       /* Map<String, String> columns = rowsData.stream().map(rowData -> {
            String columnName = rowData.getColumns().get("COLUMN_NAME").getValue();
            String tableName = rowData.getColumns().get("TABLE_NAME").getValue();
            return new AbstractMap.SimpleEntry<String, String>(columnName, tableName);
        }).collect(Collectors.toMap(Map.Entry :: getKey, Map.Entry:: getValue));*/


        resultSet.beforeFirst();

        return ResultSetData.builder()
                .schemaName(metaData.getSchemaName(1))
                .tableName(metaData.getTableName(1))
                .columnCount(metaData.getColumnCount())
                .rowsData(rowsData)
                .interceptionPoint(interceptionPoint)
                .build();
    }
}
