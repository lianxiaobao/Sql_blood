package com.diven.common.hive.blood.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @ClassName ConfigUtil
 * @Description TODO
 * @Autor yanni
 * @Date 2020/5/9 11:24
 * @Version 1.0
 **/
public class ConfigUtil {

    //创建config对象
    private static Config config  = ConfigFactory.load();

    public static String getUserName(){
        return config.getString("neo4j.userName");
    }

    public static String getPassword(){
        return config.getString("neo4j.password");
    }

    public static String getUrl(){
        return config.getString("neo4j.url");
    }
}
