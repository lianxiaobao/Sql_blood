package com.diven.common.hive.blood.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *  1. 获取数据源
 *  2. 获取连接对象
 *  3. 返回dbutils的QueryRunner对象
 * @ClassName JDBCUtil
 * @Description TODO
 * @Autor yanni
 * @Date 2020/5/5 12:17
 * @Version 1.0
 */
public class JDBCUtil {
    private static ComboPooledDataSource dataSource = null;
    static {
        dataSource = new ComboPooledDataSource();
    }
    /**
     * 1.获取数据库连接对象 （事务），这个连接对象需要手动释放
     * @return
     */
    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 2.获取dbutils的QueryRunner对象（非事务），资源会自动释放
     * @return
     */
    public static QueryRunner getQueryRunner() {
        return new QueryRunner(dataSource);
    }
}
