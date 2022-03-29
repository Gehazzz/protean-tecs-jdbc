package com.protean.tecs.jdbc;

import com.protean.tecs.jdbc.client.AnalysisServiceClient;
import com.protean.tecs.jdbc.dto.ColumnData;
import com.protean.tecs.jdbc.dto.ResultSetData;
import com.protean.tecs.jdbc.dto.RowData;
import com.sun.rowset.CachedRowSetImpl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import javax.sql.rowset.CachedRowSet;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Test {
    /*  public static void main(String[] args) {
          ClassLoader loader = Driver.class.getClassLoader();
          ProteanTecsJdbcDriver.class.getClassLoader()..classes.stream().filter(cls -> cls.getName().contains("mysql") || cls.getName().contains("vertica")).collect(Collectors.toList())
          try {
              loader.getResources("");
          } catch (IOException e) {
              e.printStackTrace();
          }
      }*/
    public static void main(String[] args) throws IOException {
        try {
            //jdbc:proteantecs://localhost:3306/devdb
            //Driver driver = (Driver) Class.forName("com.protean.tecs.jdbc.ProteanTecsJdbcDriver").newInstance();
            //Connection conn = DriverManager.getConnection("jdbc:proteantecs://localhost:5433/docker", "dbadmin", "");
            Connection conn = DriverManager.getConnection("jdbc:proteantecs://localhost:3306/devdb", "root", "root");
         /*   conn.createStatement().execute("select * from failedpn");

            ResultSet schemas = conn.getMetaData().getCatalogs();
            CachedRowSet cachedRowSet = new CachedRowSetImpl();
            cachedRowSet.populate(schemas);
            System.out.println(resultSetToString(cachedRowSet));
            //System.out.println(resultSetToString(cachedRowSet.getOriginal()));
            cachedRowSet.beforeFirst();
            ResultSet modified = modifyResultSet(cachedRowSet);
            System.out.println(resultSetToString(modified));
            modified.beforeFirst();
            System.out.println(resultSetToDto(modified, Thread.currentThread().getStackTrace()[1].getMethodName()));

            AnalysisServiceClient client = new AnalysisServiceClient(new OkHttpClient.Builder()
                    .addInterceptor(userAgent("ProteanTecsDriver" + "/" + "1.0"))
                    .socketFactory(new SocketChannelSocketFactory())
                    .build());

            modified.beforeFirst();*/
            //conn.createStatement().execute("INSERT INTO failedpn (metric_date, metric_hour, game_id, game_id_str, user_id, event_ts, bundle_id, mkt, mkt_str, device_token) VALUES (20171103, 2017110301, 23, 'niso', 1, '2017-11-03 00:01:01', 'bundle-1', 1, 'apple', 'abcdef');");

            PreparedStatement preparedStatement = conn.prepareStatement("select * from failedpn");
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            //ResultSet resultSet = preparedStatement.executeQuery();
            System.out.println(metaData);
            //client.getColumnsToHide(resultSetToDto(modified, Thread.currentThread().getStackTrace()[1].getMethodName()));
            /*conn.getMetaData().getColumns("devdb", null, "failedpn", "%");
            client.sendLog("log from main");
            conn.getMetaData().getTables("devdb", null, null, new String[]{"LOCAL TEMPORARY", "TABLE", "VIEW"});*/
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static String resultSetToString(ResultSet resulSet) {
        ResultSetMetaData metaData = null;
        List<String> rows = new ArrayList<>();
        try {
            metaData = resulSet.getMetaData();
            while (resulSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.add(metaData.getColumnName(i) + "/" + metaData.getColumnLabel(i) + ": " + resulSet.getString(i));
                }
                String strRow = "{" + String.join(", ", row) + "}";
                rows.add(strRow);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return String.join(", ", rows);
    }

    public static ResultSet modifyResultSet(CachedRowSet resultSet) throws SQLException {
        while (resultSet.next()) {
            String value = resultSet.getString("TABLE_CAT");
            if (value.equals("devdb")) {
                //resultSet.deleteRow();
                resultSet.updateString("TABLE_CAT", "devdb_modified");
            }
        }
        //resultSet.acceptChanges();
        resultSet.beforeFirst();
        return resultSet;
    }

    public static ResultSetData resultSetToDto(ResultSet resultSet, String interceptionPoint) throws SQLException {
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
            RowData rowData = RowData.builder()
                    .columns(columns)
                    .build();
            rowsData.add(rowData);
        }
        Map<String, String> columns = rowsData.stream()
                .findAny()
                .map(RowData::getColumns)
                .orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));

        resultSet.beforeFirst();

        return ResultSetData.builder()
                .schemaName(metaData.getSchemaName(1))
                .tableName(metaData.getTableName(1))
                .columnCount(metaData.getColumnCount())
                .rowsData(rowsData)
                .interceptionPoint(interceptionPoint)
                .build();
    }

    public static Interceptor userAgent(String userAgent) {
        return chain -> chain.proceed(chain.request().newBuilder()
                .header("User-Agent", userAgent)
                .build());
    }
}
