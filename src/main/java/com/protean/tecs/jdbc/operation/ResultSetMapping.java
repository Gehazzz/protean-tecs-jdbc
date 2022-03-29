package com.protean.tecs.jdbc.operation;

import com.protean.tecs.jdbc.dto.ResultSetData;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetMapping<T> {
    T resultSetToDto(ResultSet resultSet) throws SQLException;
    T resultSetToDto(ResultSet resultSet, String interceptionPoint) throws SQLException;
}
