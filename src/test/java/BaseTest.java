import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import junit.framework.TestCase;

public class BaseTest extends TestCase{
	
	static String hqls [] = {
//			"create table temp.b1(id string, name string) row format delimited fields terminated by ',';",
//			"create table temp.b2(id string, age int) row format delimited fields terminated by ',';",
//			"create table temp.c1(cid string, cname string) row format delimited fields terminated by ',';",
//			"create table temp.c2(id string, age int) row format delimited fields terminated by ',';" ,
//			"create table temp.d1(id string, name string, age int) row format delimited fields terminated by ',';",
			"insert overwrite table temp.c1 select id as cid, name as cname from temp.b1;",
			"insert overwrite table temp.c2 select id, age from temp.b2;" ,
			"insert overwrite table temp.d1 select t1.id, t1.name, t2.age from temp.c1 t1 join temp.c2 t2 on t1.id = t2.id;",
			"insert into table temp.e1 select t1.id,t2.name,t3.aid from temp.d1 t1 left join (select id from temp.a2 a2 join temp.b1 b1 on a2.id=b1.id) t2 on t1.id = t2.id left join temp.a1 t3 on t1.id=t3.aid"
			//"select c1, max(c2) as max_c2 from test_table group by c1;",
			//"insert into table c  SELECT line.monitor_unit_id, sb.*, a.TQ, a.WD, a.SD, a.FJ, a.JSL, a.sjsj,  gz.gzsj FROM `tb_qxzh_xl_lxsb` sb,line, tb_qxzh_gz_pwxlgz gz,(SELECT  q.*, r.PMS_DWID  FROM  pdwqy_qxhj_qxsj q,  pdwqy_qxhj_dwb_referece r  WHERE   q.dwid = r.dwid ) a WHERE sb.obj_id = gz.pms_id AND sb.obj_id = line.pms_line_oid AND sb.ywdw = a.PMS_DWID AND a.sjsj = date(gzsj)"
//
//   " insert into table c   select a.id,b.id from a join b on a.id=b.id ;"
		//"insert into table MW_SYS.MWT_PD_DEPS select * from mw_app.DYT_DEPTSCONFIG;"
	};
	

	/**
	 * 输出标准的json字符串
	 * @param obj
	 */
	public static void printJsonString(Object obj) {
		String str = JSON.toJSONString(obj, 
				SerializerFeature.WriteMapNullValue, 
				SerializerFeature.WriteNullListAsEmpty, 
				SerializerFeature.DisableCircularReferenceDetect, 
				SerializerFeature.PrettyFormat);
		System.out.println(str);
	}
}
