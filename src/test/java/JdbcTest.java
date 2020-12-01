import com.diven.common.hive.blood.api.HiveBloodEngine;
import com.diven.common.hive.blood.api.HiveBloodEngineImpl;
import com.diven.common.hive.blood.graph.GraphUI;
import com.diven.common.hive.blood.model.HiveField;
import com.diven.common.hive.blood.model.HiveTable;
import com.diven.common.hive.blood.model.TableBlood;
import com.diven.common.hive.blood.utils.JDBCUtil;
import com.diven.common.hive.toGraph.BloodBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @ClassName JdbcTest
 * @Description TODO
 * @Autor yanni
 * @Date 2020/5/29 11:27
 * @Version 1.0
 **/
public class JdbcTest {

    private static HiveBloodEngine bloodEngine = new HiveBloodEngineImpl();

    public static void main(String[] args) throws Exception {
        ArrayList<String> strings = new ArrayList<>();

        String targetTable = null;
        String targetColumn = null;
        String sourceTable = null;
        String sourceColumn = null;

        String sql = "select DISTINCT BM,ZDM from tb_zdhpd_bjgxb";
        QueryRunner qr = JDBCUtil.getQueryRunner();
        List<BloodBean> tarBeanList = qr.query(sql, new BeanListHandler<BloodBean>(BloodBean.class));

        for (BloodBean bloodBean:tarBeanList){
            targetTable = bloodBean.getBM().toLowerCase();
            targetColumn = bloodBean.getZDM().toLowerCase();
            //System.out.println(targetColumn);

            String newSQL= "select DISTINCT GXBM,GXBZDM from tb_zdhpd_bjgxb where BM = ? and ZDM = ? ";
            String[] params = { targetTable,targetColumn};
            List<BloodBean> souBeanList = qr.query(newSQL, new BeanListHandler<BloodBean>(BloodBean.class),params);
            for (BloodBean souBloodBean:souBeanList){
                sourceTable = souBloodBean.getGXBM().toLowerCase();
                sourceColumn = souBloodBean.getGXBZDM().toLowerCase();

                String sqlStr = "create table "+targetTable+" ("+targetColumn+" string);insert into table "+targetTable+" select "+sourceColumn+" from "+sourceTable;
                strings.add(sqlStr);
            }
        }

//        for (Map<String, Object> stringObjectMap : mapList) {
//            for (String key : stringObjectMap.keySet()){
//                name = (String)stringObjectMap.get(key);
//
//                String newSQL= "select name2 from blood where name1 = ?";
//
//                List<Map<String, Object>> query = qr.query(newSQL, new MapListHandler(),name);
//                for (Map<String, Object> stringMap : query) {
//                    for (String newkey : stringMap.keySet()){
//                        name2 = (String)stringMap.get(newkey);
//
//                        String sqlStr = "insert into table "+name+" select * from "+name2;
//                        strings.add(sqlStr);
//                    }
//                }
//            }
//        }

        TableBlood tableBlood = bloodEngine.getTableBlood(strings);
        //FileUtils.writeStringToFile(new File("E://csv2.csv"), Json2Csv(string));
        GraphUI.show(tableBlood);
        System.in.read();

        List<HiveField> fields = bloodEngine.getTableFields(strings, new HiveTable("temp", "c1"));
        GraphUI.show(fields);
        System.in.read();
    }

}
