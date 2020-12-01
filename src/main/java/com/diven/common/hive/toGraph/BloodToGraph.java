package com.diven.common.hive.toGraph;

import com.diven.common.hive.blood.api.HiveBloodEngine;
import com.diven.common.hive.blood.api.HiveBloodEngineImpl;
import com.diven.common.hive.blood.model.*;
import com.diven.common.hive.blood.utils.JDBCUtil;
import com.diven.common.hive.blood.utils.Neo4jUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName BloodToGraph
 * @Description TODO 使用注意:
 *                            1、两表关联查询使用join，不能使用逗号;
 *                            2、数据表前面要加上数据库名称：数据库.表名(例如us_app.user)
 * @Autor yanni
 * @Date 2020/5/9 11:09
 * @Version 1.0
 **/
public class BloodToGraph {

    private static HiveBloodEngine bloodEngine = new HiveBloodEngineImpl();
    private static ArrayList<String> strings = new ArrayList<>();
    private static QueryRunner qr = null;
    private static ArrayList<String> listTmp = null;
    private static HashMap<String,String> tableAndYwxtmc = null;

    /**
     * 获取SQL语句列表
     * @param source
     * @param target
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static ArrayList<String> getListSQL( String source ,String target ,String tableName) throws SQLException {
        String targetTable = null;
        String sourceTable = null;
        strings = new ArrayList<>();

        String sql = "select DISTINCT "+target+" from "+tableName;
        qr = JDBCUtil.getQueryRunner();
        List<BloodBean> tarBeanList = qr.query(sql, new BeanListHandler<BloodBean>(BloodBean.class));

        for (BloodBean bloodBean:tarBeanList){
            targetTable = bloodBean.getBM();
            //System.out.println(targetColumn);

            String newSQL= "select DISTINCT "+source+" from "+tableName+" where "+target+" = ? ";
            String[] params = { targetTable};
            List<BloodBean> souBeanList = qr.query(newSQL, new BeanListHandler<BloodBean>(BloodBean.class),params);
            for (BloodBean souBloodBean:souBeanList){
                sourceTable = souBloodBean.getGXBM();

                String sqlStr = "insert into table "+targetTable+" select * from "+sourceTable+";";
                strings.add(sqlStr);
            }
        }

        return strings;
    }

    /**
     * 获取全部的表名
     * @param source
     * @param target
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static Set<String> getAllTables( String source ,String target ,String tableName) throws SQLException {
        String targetTable = null;
        String sourceTable = null;
        Set<String> strings = new HashSet<>();

        String sql = "select DISTINCT "+target+" from "+tableName;
        qr = JDBCUtil.getQueryRunner();
        List<BloodBean> tarBeanList = qr.query(sql, new BeanListHandler<BloodBean>(BloodBean.class));

        for (BloodBean bloodBean:tarBeanList){
            targetTable = bloodBean.getBM().toLowerCase();
            //String tableColumn = getTableColumn(targetTable);
            //System.out.println(targetColumn);
            strings.add(targetTable);


        }
        String newSQL= "select DISTINCT "+source+" from "+tableName;
        List<BloodBean> souBeanList = qr.query(newSQL, new BeanListHandler<BloodBean>(BloodBean.class));
        for (BloodBean souBloodBean:souBeanList){
            sourceTable = souBloodBean.getGXBM().toLowerCase();
            //String tableColumn1 = getTableColumn(sourceTable);

            strings.add(sourceTable);
        }

        return strings;
    }

    /**
     * 获取所有的表名以及其所在的业务系统
     * @param source
     * @param target
     * @param tableName
     * @return
     */
    public static HashMap<String,String> getAllTablesAndYWXT(String source ,String target ,String tableName) throws SQLException {
        String targetTable = null;
        String sourceTable = null;
        String tarYwxtmc = null;
        String souYwxtmc = null;
        tableAndYwxtmc = new HashMap<String,String>();

        String sql = "select DISTINCT YWXTMC,"+target+" from "+tableName;
        qr = JDBCUtil.getQueryRunner();
        List<BloodBean> tarBeanList = qr.query(sql, new BeanListHandler<BloodBean>(BloodBean.class));

        for (BloodBean bloodBean:tarBeanList){
            targetTable = bloodBean.getBM().toLowerCase();
            tarYwxtmc = bloodBean.getYWXTMC();

            tableAndYwxtmc.put(targetTable,tarYwxtmc);
        }

        String newSQL= "select DISTINCT YWXTMC,"+source+" from "+tableName;
        List<BloodBean> souBeanList = qr.query(newSQL, new BeanListHandler<BloodBean>(BloodBean.class));
        for (BloodBean souBloodBean:souBeanList){
            sourceTable = souBloodBean.getGXBM().toLowerCase();
            souYwxtmc = souBloodBean.getYWXTMC();

            tableAndYwxtmc.put(sourceTable,souYwxtmc);
        }

        return tableAndYwxtmc;
    }

    /**
     * 向各节点添加表的业务系统名称
     * @param tablesAndYwxtmc
     */
    public static void ywxtToNode(HashMap<String,String> tablesAndYwxtmc){
        String dbName = null;
        String new_tableName = null;

        Iterator<Map.Entry<String, String>> it=tablesAndYwxtmc.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String, String> entry=it.next();
            String tableName = entry.getKey();

            if (tableName.contains(".")){
                String[] split = tableName.split("\\.");
                dbName = split[0];
                new_tableName = split[1];
            }else{
                dbName = "default";
                new_tableName = tableName;
            }

            String ywxtmc = entry.getValue();

            Neo4jUtil.ywxtToNode(dbName,new_tableName,ywxtmc);
        }
    }

    /**
     * 获取表的所有字段(Oracle)
     * @param tableName
     */
    public static ArrayList<String> getTableColumn(String tableName){
        String subStr = null;
        String sql = "select tt.column_name from user_col_comments tt where tt.table_name=upper('"+tableName.toUpperCase()+"')";
        //String sql = "select field from bloodField;";
        ArrayList<String> strings = new ArrayList<>();
        qr = JDBCUtil.getQueryRunner();
        try {
            List<Object[]> list = qr.query(sql, new ArrayListHandler());
            for(Object[] obj:list){
                for (Object o:obj){
                    strings.add(o.toString());
                }
            }
            return strings;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void columnsToGraph(ArrayList<String> tableNames) throws SQLException {
        String dbName = null;
        String new_tableName = null;

        for (String tableName:tableNames){

            if (tableName.contains(".")){
                String[] split = tableName.split("\\.");
                dbName = split[0];
                new_tableName = split[1];
            }else{
                dbName = "default";
                new_tableName = tableName;
            }
            ArrayList<String> tableColumns = getTableColumn(tableName);
            for (String column:tableColumns){
                Neo4jUtil.createAttrNodes(dbName,new_tableName,column);
                Neo4jUtil.createAttrEdges(dbName,new_tableName,column);
            }
        }
    }

    /**
     * 将表的血缘关系传到Neo4j
     * @throws Exception
     */
    public static void putTableBloodToGraph(List<String> sqls) throws Exception {
        //System.out.println("----------------------1----------------");
        TableBlood tableBlood = bloodEngine.getTableBlood(sqls);
        //System.out.println("----------------------2----------------");

        for(HiveTableNode node : tableBlood.getNodes()) {
            String tableName = node.getTableName();
            Integer id = node.getId();
            String dbName = node.getDbName();
            Neo4jUtil.createNodes(tableName,id,dbName);

            // FIXME: 2020/5/9 字段的血缘关系不完善(* 无法解析)
            //getFieldBloodByTable(sqls,dbName,tableName);
//            List<HiveField> fields = bloodEngine.getTableFields(sqls, new HiveTable(dbName, tableName));
//            for(HiveField field : fields) {
//                Neo4jUtil.createAttrNodes(tableName,field.getFieldName());
//                Neo4jUtil.createAttrEdges(tableName,field.getFieldName());
//            }
        }

        for(HiveTableEdge edge : tableBlood.getEdges()) {
            HiveTableNode source = edge.getSource();
            String sourceTableName = source.getTableName();
            Integer sourceId = source.getId();
            String sourceDbName = source.getDbName();
            //String newSourceTableName = sourceDbName+"."+sourceTableName;

            HiveTableNode target = edge.getTarget();
            String targetTableName = target.getTableName();
            Integer targetId = target.getId();
            String targetDbName = target.getDbName();
            //String newTargetTableName = targetDbName+"."+targetTableName;

            Neo4jUtil.createEdges(sourceId,sourceTableName,targetId,targetTableName);
        }
    }

    /**
     * 添加属性
     * @param source
     * @param souColumn
     * @param tableName
     * @throws SQLException
     */
//    public static void souNodeAddColumn(String source ,String souColumn,String tableName) throws SQLException {
//        String dbName = null;
//        String newTableName = null;
//
//        String sql = "select distinct "+source+" from "+tableName;
//
//        List<Map<String, Object>> mapList = qr.query(sql, new MapListHandler());
//
//        for (Map<String, Object> stringObjectMap : mapList) {
//            for (String key : stringObjectMap.keySet()){
//                String sourceTable = (String)stringObjectMap.get(key);
//
//                String newSQL= "select distinct "+souColumn+" from "+tableName+" where "+source+" = ?";
//
//                List<Map<String, Object>> query = qr.query(newSQL, new MapListHandler(),sourceTable);
//                for (Map<String, Object> stringMap : query) {
//                    for (String newkey : stringMap.keySet()){
//                        String sourceColumn = (String)stringMap.get(newkey);
//
//                        if (sourceTable.contains(".")){
//                            String[] split = sourceTable.split("\\.");
//                            dbName = split[0];
//                            newTableName = split[1];
//
//                        }else{
//                            dbName = "default";
//                            newTableName = sourceTable;
//
//                        }
//
//                        Neo4jUtil.addColumn(dbName,newTableName,sourceColumn);
//                    }
//                }
//            }
//        }
//    }

    /**
     * 添加属性
     * @param target
     * @param tarColumn
     * @param tableName
     * @throws SQLException
     */
//    public static void tarNodeAddColumn(String target ,String tarColumn,String tableName) throws SQLException {
//
//        qr = JDBCUtil.getQueryRunner();
//        String dbName = null;
//        String newTableName = null;
//
//        String sql = "select distinct "+target+" from "+tableName;
//
//        List<Map<String, Object>> mapList = qr.query(sql, new MapListHandler());
//
//        for (Map<String, Object> stringObjectMap : mapList) {
//            for (String key : stringObjectMap.keySet()){
//                String targetTable = (String)stringObjectMap.get(key);
//
//                String newSQL= "select distinct "+tarColumn+" from "+tableName+" where "+target+" = ?";
//
//                List<Map<String, Object>> query = qr.query(newSQL, new MapListHandler(),targetTable);
//                for (Map<String, Object> stringMap : query) {
//                    for (String newkey : stringMap.keySet()){
//                        String targetColumn = (String)stringMap.get(newkey);
//
//                        if (targetTable.contains(".")){
//                            String[] split = targetTable.split("\\.");
//                            dbName = split[0];
//                            newTableName = split[1];
//                        } else{
//                            dbName = "default";
//                            newTableName = targetTable;
//                        }
//
//                        Neo4jUtil.addColumn(dbName,newTableName,targetColumn);
//
//                    }
//                }
//            }
//        }
//    }

    /**
     * 给边添加关联字段属性
     * @param source
     * @param souColumn
     * @param target
     * @param tarcolumn
     * @param tableName
     * @throws SQLException
     */
    public static void edgeAddAttr(String source ,String souColumn,String target,String tarcolumn,String tableName) throws SQLException {
        String newtargetTable = null;
        String newsourceTable = null;
        qr = JDBCUtil.getQueryRunner();
        String sql = "select distinct "+source+" from "+tableName;
        List<Map<String, Object>> mapList = qr.query(sql, new MapListHandler());
        for (Map<String, Object> stringObjectMap : mapList) {
            for (String key : stringObjectMap.keySet()){
                String sourceTable = (String)stringObjectMap.get(key);
                String newSQL= "select distinct "+target+" from "+tableName+" where "+source+" = ?";
                List<Map<String, Object>> query = qr.query(newSQL, new MapListHandler(),sourceTable);
                for (Map<String, Object> stringMap : query) {
                    for (String newkey : stringMap.keySet()){
                        String targetTable = (String)stringMap.get(newkey);
                        String cloumnSQL= "select distinct "+souColumn+","+tarcolumn+" from "+tableName+" where "+source+" = ? and "+target+" = ? ";
                        String[] params = { sourceTable,targetTable};
                        List<Map<String, Object>> columns = qr.query(cloumnSQL, new MapListHandler(),params);
                        for (Map<String, Object> couAndTarCol:columns){
                            String sourceValue = (String) couAndTarCol.get(souColumn);
                            String targetValue = (String) couAndTarCol.get(tarcolumn);

                            if (sourceTable.contains(".")){
                                String[] split = sourceTable.split("\\.");
                                newsourceTable = split[1];
                            } else{
                                newsourceTable = sourceTable;
                            }

                            if (targetTable.contains(".")){
                                String[] split = targetTable.split("\\.");
                                newtargetTable = split[1];
                            } else{
                                newtargetTable = targetTable;
                            }

                            //System.out.println(sourceValue+"----------"+targetValue);
                            Neo4jUtil.edgeAddAttr(newsourceTable,newtargetTable,sourceValue,targetValue);
                        }
                    }
                }
            }
        }
    }

    /**
     * 添加字段血缘
     * @param SQLList
     * @param dbName
     * @param tableName
     */
    public static void getFieldBloodByTable(List<String> SQLList,String dbName,String tableName){
        try {
            FieldBlood fieldBlood = bloodEngine.getFieldBloodByTable(SQLList, new HiveTable(dbName, tableName));

            Set<FieldBloodTree> bloodTrees = new HashSet<>(fieldBlood.values());
            for(FieldBloodTree bloodTree : bloodTrees) {
                String bloodTreeDbName = bloodTree.getDbName();
                String bloodTreeTableName = bloodTree.getTableName();
                String bloodTreeFieldName = bloodTree.getFieldName();

                Neo4jUtil.createAttrNodes(bloodTreeDbName,bloodTreeTableName,bloodTreeFieldName);
                Neo4jUtil.createAttrEdges(bloodTreeDbName,bloodTreeTableName,bloodTreeFieldName);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 主方法
     * @param source
     * @param souColumn
     * @param target
     * @param tarColumn
     * @param tableName
     */
    public static void run(String source ,String souColumn,String target ,String tarColumn,String tableName){

        try {
            ArrayList<String> listSQL = getListSQL(source, target, tableName);
            //ArrayList<String> allTables = getAllTables(source, target, tableName);

            //表级血缘
            for(String sql:listSQL){
                listTmp = new ArrayList<String>();
                listTmp.add(sql);
                putTableBloodToGraph(listTmp);

                listTmp.clear();
            }

            //给表级血缘的边添加属性(来源表、目标表、关联字段)
            edgeAddAttr(source,souColumn,target,tarColumn,tableName);
            //souNodeAddColumn(source,souColumn,tableName);
            //tarNodeAddColumn(target,tarColumn,tableName);

            //添加字段节点，并将字段节点与表节点关联
            //columnsToGraph(allTables);

            //向各节点添加业务系统属性
            ywxtToNode(getAllTablesAndYWXT(source,target,tableName));
            Neo4jUtil.closeConnect();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
