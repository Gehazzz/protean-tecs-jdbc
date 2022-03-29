package com.protean.tecs.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.protean.tecs.jdbc.dto.*;
import com.sun.rowset.CachedRowSetImpl;
import okhttp3.*;

import javax.sql.rowset.CachedRowSet;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AnalysisServiceClient {
    //private final String BASE_URL = "http://localhost:8081";
    private final String SCHEME = "http";
    private final String HOST = "localhost";
    private final int PORT = 8081;
    private final String VERSION = "v1";
    private final HttpUrl BASE_URL = new HttpUrl.Builder()
            .scheme(SCHEME)
            .host(HOST)
            .port(PORT)
            .addPathSegment(VERSION)
            .build();
    private final HttpUrl APPROVE_URL = BASE_URL.newBuilder()
            .addPathSegment("approve")
            .build();
    private final HttpUrl COLUMNS_BASE_URL = BASE_URL.newBuilder()
            .addPathSegment("columns")
            .build();
    private final HttpUrl TABLES_BASE_URL = BASE_URL.newBuilder()
            .addPathSegment("tables")
            .build();

    private final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final OkHttpClient httpClient;

    private final ObjectMapper objectMapper;

    public AnalysisServiceClient(OkHttpClient httpClient) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    public QueryApprovalResponse getApproveForQuery (QueryRequestForApproval queryRequestForApproval) throws IOException {
        return objectMapper.readValue(postObjectAsJson(queryRequestForApproval, APPROVE_URL), QueryApprovalResponse.class);
    }

    public ColumnsToHide getColumnsToHide(ResultSetData data, String db) throws IOException {
        HttpUrl columnsUrl = COLUMNS_BASE_URL.newBuilder()
                .addPathSegment(db)
                .addQueryParameter("action", "hide")
                .build();
        return objectMapper.readValue(postObjectAsJson(data, columnsUrl), ColumnsToHide.class);
    }

    public TablesToHide getTablesToHide(ResultSetData data, String db) throws IOException {
        HttpUrl columnsUrl = TABLES_BASE_URL.newBuilder()
                .addPathSegment(db)
                .addQueryParameter("action", "hide")
                .build();
        return objectMapper.readValue(postObjectAsJson(data, columnsUrl), TablesToHide.class);
    }


    public void sendLog(String log) throws IOException {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        String className = Thread.currentThread().getStackTrace()[2].getClassName();

        HttpUrl logUrl = BASE_URL.newBuilder()
                .addPathSegment("log")
                .build();

        LogMessage logMessage = LogMessage.builder()
                .message(log)
                .interceptionPoint(className.concat(".").concat(methodName))
                .build();
        postObjectAsJson(logMessage, logUrl);
    }

    private String postObjectAsJson(Object object, HttpUrl httpUrl) throws IOException {
        //String json = "{\"id\":1,\"name\":\"John\"}";
       // Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINE);
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(object);
        System.out.println(json);
        RequestBody body = RequestBody.create(json,
                MediaType.parse("application/json"));

        Request request = new Request.Builder()
                .url(httpUrl)
                .post(body)
                .build();

        Call call = httpClient.newCall(request);
        ResponseBody response = call.execute().body();

        ResponseBody responseBody = Objects.requireNonNull(response);
        String string =  responseBody.string();

        return string;
    }

/*    private String postObjectAsJson(Object object, HttpUrl httpUrl) throws IOException {
        //String json = "{\"id\":1,\"name\":\"John\"}";
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(object);
        System.out.println(json);
        RequestBody body = RequestBody.create(json,
                MediaType.parse("application/json"));

        Request request = new Request.Builder()
                .url(httpUrl)
                .post(body)
                .build();

        Call call = httpClient.newCall(request);
        ResponseBody response = call.execute().body();
        return Objects.requireNonNull(response).string();
    }*/


    public ResultSet sendResultSet(ResultSet resultSet) throws SQLException {
        CachedRowSet cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSet);
        sendQuery(resultSetToString(cachedRowSet));
        return cachedRowSet.getOriginal();
    }

    public void sendQuery(String sql) {
        try {
            String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
            String className = Thread.currentThread().getStackTrace()[2].getClassName();
            sendForAnalysis("Executing sql from method: " + className + "." + methodName + ", Query: " + sql);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Response sendForAnalysis(String data) throws IOException {

       /* RequestBody formBody = new FormBody.Builder()
                .add("data", data)
                .build();*/
        RequestBody body = RequestBody.create(data,
                MediaType.parse("text/plain"));

        Request request = new Request.Builder()
                .url("http://localhost:8081" + "/v1/analysis")
                //.addHeader("Accept", "*/*")
                .addHeader("content-type", "text/plain")
                .addHeader("Accept-Encoding", "gzip")
                //.addHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                //.addHeader("Accept", "application/json, text/javascript, */*; q=0.01")
                //.addHeader("Accept-Language", "en-US,en;q=0.8,fa;q=0.6,ar;q=0.4")
                .post(body)
                .build();

        Call call = httpClient.newCall(request);
        Response response = call.execute();
        return response;
    }

    public String resultSetToString(ResultSet resulSet) throws SQLException {
        ResultSetMetaData metaData = null;
        List<String> rows = new ArrayList<>();
        try {
            metaData = resulSet.getMetaData();
        while (resulSet.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row.add(metaData.getColumnName(i) + "/" + metaData.getColumnLabel(i) + ": " +resulSet.getString(i));
            }
            String strRow = "{" + String.join(", ", row) + "}";
            rows.add(strRow);
        }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        resulSet.beforeFirst();
        return String.join(", ", rows);
    }
}
