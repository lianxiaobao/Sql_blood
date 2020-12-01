package com.diven.common.hive.blood.utils;

import java.sql.*;

/**
 * @ClassName Neo4jUtil
 * @Description TODO
 * @Autor yanni
 * @Date 2020/5/8 14:27
 * @Version 1.0
 **/
public class Neo4jUtil {

    private static String USERNAME = ConfigUtil.getUserName();
    // 定义数据库的密码
    private static String PASSWORD = ConfigUtil.getPassword();
    // 定义访问数据库的地址
    private static String URL = ConfigUtil.getUrl();

    private static Connection conn;
    static {
        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createNodes(String tableName, Integer id, String dbName) {
        try {
            create(tableName, id, dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createEdges(Integer sourceId, String sourceTableName, Integer targetId, String targetTableName) {
        try {
            create(sourceId, sourceTableName, targetId, targetTableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void create(String tableName, Integer id, String dbName) throws SQLException {
        //Neo4jUtil configNeo4jDBUtil = new Neo4jUtil();
        //Connection conn = configNeo4jDBUtil.getNeo4jConnection();
        try {
            Statement statement = conn.createStatement();
            statement.execute("merge (" + tableName + ":Table { tableName:'" + tableName + "',dbName:'" + dbName + "',tid:" + id + " })");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void create(Integer sourceId, String sourceTableName, Integer targetId, String targetTableName) throws SQLException {
        //Neo4jUtil configNeo4jDBUtil = new Neo4jUtil();
        //Connection conn = configNeo4jDBUtil.getNeo4jConnection();
        try {
            Statement statement = conn.createStatement();
            statement.execute("match (" + sourceTableName + ":Table),(" + targetTableName + ":Table) \n" +
                    "where " + sourceTableName + ".tid=" + sourceId + "  and " + targetTableName + ".tid=" + targetId + " merge (" + sourceTableName + ")-[r:generate{sourceTableName:'"+sourceTableName+"',targetTableName:'"+targetTableName+"'} ]->(" + targetTableName + ") ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createAttrNodes(String dbName,String tableName, String fieldName) throws SQLException {
        //Neo4jUtil configNeo4jDBUtil = new Neo4jUtil();
        //Connection conn = configNeo4jDBUtil.getNeo4jConnection();
        try {
            Statement statement = conn.createStatement();
            statement.execute("merge (" + fieldName + ":Field { dbName:'"+dbName+"',fieldName:'" + fieldName + "',tableName:'" + tableName + "' })");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createAttrEdges(String dbName,String tableName,String fieldName){
        try {
            Statement statement = conn.createStatement();
            statement.execute("MATCH (" + fieldName + ":Field),(" + tableName + ":Table) \n" +
                    " where "+fieldName+".tableName="+tableName+".tableName"+
                    " and "+fieldName+".dbName="+tableName+".dbName"+
                    " merge (" + fieldName + ")-[r:attr ]->(" + tableName + ") ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void edgeAddAttr(String sourceTable,String targetTable,String sourceColumn,String targetColumn){
        try {
            Statement statement = conn.createStatement();
            statement.execute("MATCH (:Table)-[r:generate]->(:Table) \n" +
                    " where r.sourceTableName = '"+sourceTable+"' and r.targetTableName = '" +targetTable +"'"+
                    " SET r.sourceColumn = '"+sourceColumn+"'");
            statement.execute("MATCH (:Table)-[r:generate]->(:Table) \n" +
                    " where r.sourceTableName = '"+sourceTable+"' and r.targetTableName = '" +targetTable +"'"+
                    " SET  r.targetColumn = '"+targetColumn+"'");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ywxtToNode(String dbName,String tableName,String ywxtmc){
        try {
            Statement statement = conn.createStatement();
            statement.execute("match (n{dbName:'"+dbName+"',tableName:'"+tableName+"'}) set n.ywxtmc='"+ywxtmc+"' return n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeConnect(){
        try {
            if (conn.isClosed()==false){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
