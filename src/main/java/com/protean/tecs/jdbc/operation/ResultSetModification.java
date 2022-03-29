package com.protean.tecs.jdbc.operation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ResultSetModification {
    ResultSet hide(ResultSet resultSet, List<String> columnsToHide) throws SQLException;
}
