package com.diven.common.hive.toGraph;

import java.sql.SQLException;

public class HiveSqlBloodFactoryTest{

	/**
	 * 需求:将数据库中的原表以及目标表以图的形式保存到neo4j，并且显示关联的字段
	 * source：原表  souColumn：原表关联  target：目标表  tarColumn:目标表关联字段  tableName：数据存在的表
	 *
	 * */
	public static void main(String[] args) throws SQLException {

		BloodToGraph.run("GXBM","soucolumn","BM","tarcolumn","blood");
		//BloodToGraph.edgeAddAttr("GXBM","soucolumn","BM","tarcolumn","blood");
	}
}
