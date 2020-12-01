import com.diven.common.hive.blood.utils.Neo4jUtil;
import com.diven.common.hive.toGraph.BloodToGraph;

import java.util.*;

public class HiveSqlBloodFactoryTest extends BaseTest{

	/**
	 * 需求:将数据库中的原表以及目标表以图的形式保存到neo4j，并且显示关联的字段
	 * source：原表  souColumn：原表关联  target：目标表  tarColumn:目标表关联字段  tableName：数据存在的表
	 *
	 * */
	public void testGetTableBlood() throws Exception{

		//BloodToGraph.run("name2",null,"name1",null,"blood");
		Set<String> allTables = BloodToGraph.getAllTables("GXBM", "BM", "tb_zdhpd_bjgxb_copy");
		System.out.println(allTables.size());

		HashMap<String, String> allTablesAndYWXT = BloodToGraph.getAllTablesAndYWXT("GXBM", "BM", "tb_zdhpd_bjgxb_copy");
		Iterator<Map.Entry<String, String>> it=allTablesAndYWXT.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<String, String> entry=it.next();
			String tableName = entry.getKey();
			String ywxtmc = entry.getValue();

			System.out.println(tableName+"------"+ywxtmc);
		}

		//BloodToGraph.run("GXBM","GXBZDM","BM","ZDM","tb_zdhpd_bjgxb_copy");

	}
}
