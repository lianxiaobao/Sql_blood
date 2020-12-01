import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.diven.common.hive.blood.api.HiveBloodEngine;
import com.diven.common.hive.blood.api.HiveBloodEngineImpl;
import com.diven.common.hive.blood.graph.GraphUI;
import com.diven.common.hive.blood.model.*;
import com.diven.common.hive.blood.utils.Neo4jUtil;

public class HiveBloodEngineTest extends BaseTest{
	
	private HiveBloodEngine bloodEngine = new HiveBloodEngineImpl();
	
	/**
	 * 获取当前sql的表血缘
	 */
	public void testGetTableBlood() throws Exception{
		//TableBlood tableBlood = bloodEngine.getTableBlood(Arrays.asList(hqls));
		TableBlood tableBlood = bloodEngine.getTableBlood(Arrays.asList(hqls));
		printJsonString(tableBlood);
		//FileUtils.writeStringToFile(new File("E://csv2.csv"), Json2Csv(string));
		GraphUI.show(tableBlood);
		System.in.read();
	}
	
	/**
	 * 根据血缘图获取指定表的属性字段
	 */
	public void testGetTableFields() throws Exception{
		List<HiveField> fields = bloodEngine.getTableFields(Arrays.asList(hqls), new HiveTable("temp", "c1"));
		printJsonString(fields);
		GraphUI.show(fields);
		System.in.read();
	}
	
	/**
	 * 根据血缘图获取指定表的字段血缘
	 */
	public void testGetFieldBloodByTable() throws Exception{
		FieldBlood fieldBlood = bloodEngine.getFieldBloodByTable(Arrays.asList(hqls), new HiveTable("temp", "c1"));
		printJsonString(fieldBlood);
		GraphUI.show(fieldBlood);
		System.in.read();
	}

	/**
	 * 将表的血缘关系传到Neo4j
	 * @throws Exception
	 */
	public void testGetTableBlood2() throws Exception {
		TableBlood tableBlood = bloodEngine.getTableBlood(Arrays.asList(hqls));

		for(HiveTableNode node : tableBlood.getNodes()) {
			String tableName = node.getTableName();
			Integer id = node.getId();
			String dbName = node.getDbName();
			Neo4jUtil.createNodes(tableName,id,dbName);

			List<HiveField> fields = bloodEngine.getTableFields(Arrays.asList(hqls), new HiveTable(dbName, tableName));
			for(HiveField field : fields) {
				//Neo4jUtil.createAttrNodes(tableName,field.getFieldName());
				//Neo4jUtil.createAttrEdges(tableName,field.getFieldName());
			}
		}

		for(HiveTableEdge edge : tableBlood.getEdges()) {
			HiveTableNode source = edge.getSource();
			String sourceTableName = source.getTableName();
			Integer sourceId = source.getId();

			HiveTableNode target = edge.getTarget();
			String targetTableName = target.getTableName();
			Integer targetId = target.getId();

			Neo4jUtil.createEdges(sourceId,sourceTableName,targetId,targetTableName);
		}
	}
}
