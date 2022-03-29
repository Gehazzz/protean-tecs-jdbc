package com.protean.tecs.jdbc.operation;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class ResultSetModificationImpl implements ResultSetModification {
    @Override
    public ResultSet hide(ResultSet resultSet, List<String> columnsToHide) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String value = resultSet.getString(metaData.getColumnLabel(i));
                if (columnsToHide.contains(value))
                    resultSet.deleteRow();
            }
        }
        //resultSet.acceptChanges();
        resultSet.beforeFirst();
        return resultSet;
    }
}
