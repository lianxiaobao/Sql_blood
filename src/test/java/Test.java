import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.diven.common.hive.blood.model.Block;
import com.diven.common.hive.toGraph.BloodToGraph;

import java.util.ArrayList;

/**
 * @ClassName Test
 * @Description TODO
 * @Autor yanni
 * @Date 2020/6/8 11:29
 * @Version 1.0
 **/
public class Test {
    public static void main(String[] args) {

        String str = "{\n" +
                "    \"version\":\"1.0\",\n" +
                "    \"user\":\"root\",\n" +
                "    \"timestamp\":1592875253,\n" +
                "    \"duration\":49991,\n" +
                "    \"jobIds\":[\n" +
                "        \"job_1592874792126_0002\"\n" +
                "    ],\n" +
                "    \"engine\":\"mr\",\n" +
                "    \"database\":\"default\",\n" +
                "    \"hash\":\"12df415cfc8fd08a7d3615481ca2e151\",\n" +
                "    \"queryText\":\"insert into table c select    a.id,a.name,b.location  from a join b on a.id=b.id\",\n" +
                "    \"inputs\":[\n" +
                "        \"a\",\n" +
                "        \"b\"\n" +
                "    ],\n" +
                "    \"outputs\":[\n" +
                "        \"c\"\n" +
                "    ],\n" +
                "    \"edges\":[\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                3\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                0\n" +
                "            ],\n" +
                "            \"edgeType\":\"PROJECTION\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                4\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                1\n" +
                "            ],\n" +
                "            \"edgeType\":\"PROJECTION\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                5\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                2\n" +
                "            ],\n" +
                "            \"edgeType\":\"PROJECTION\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                3\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                0,\n" +
                "                1,\n" +
                "                2\n" +
                "            ],\n" +
                "            \"expression\":\"a.id is not null\",\n" +
                "            \"edgeType\":\"PREDICATE\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                3,\n" +
                "                6\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                0,\n" +
                "                1,\n" +
                "                2\n" +
                "            ],\n" +
                "            \"expression\":\"(a.id = b.id)\",\n" +
                "            \"edgeType\":\"PREDICATE\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"sources\":[\n" +
                "                6\n" +
                "            ],\n" +
                "            \"targets\":[\n" +
                "                0,\n" +
                "                1,\n" +
                "                2\n" +
                "            ],\n" +
                "            \"expression\":\"b.id is not null\",\n" +
                "            \"edgeType\":\"PREDICATE\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"vertices\":[\n" +
                "        {\n" +
                "            \"id\":0,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.c.id\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":1,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.c.name\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":2,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.c.location\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":3,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.a.id\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":4,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.a.name\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":5,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.b.location\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"id\":6,\n" +
                "            \"vertexType\":\"COLUMN\",\n" +
                "            \"vertexId\":\"default.b.id\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        tableBlood(str);
    }


    public static void tableBlood(String str){
        String input = null;
        String output = null;
        JSONObject object = JSON.parseObject(str);
        JSONArray inputs = object.getJSONArray("inputs");
        JSONArray outputs = object.getJSONArray("outputs");
        for (int i=0;i<inputs.size();i++){
            input = (String)inputs.get(i);
            for (int y=0;y<outputs.size();y++){
                output = (String)outputs.get(y);
            }
            //System.out.println(input+"-->"+output);
            //创建节点和边
        }
    }

    public static void columnBlood(String str){
        ArrayList<vertice> arrayList = new ArrayList<>();

        JSONObject jsonObject = JSON.parseObject(str);
        JSONArray vertices = jsonObject.getJSONArray("vertices");
        for (int i=0;i<vertices.size();i++){
            JSONObject o = (JSONObject)vertices.get(i);
            Integer id = o.getInteger("id");
            String vertexType = o.getString("vertexType");
            String vertexId = o.getString("vertexId");

            //将节点放到Java类中
            vertice vertice = new vertice();
            vertice.setId(id);
            vertice.setVertexType(vertexType);
            vertice.setVertexId(vertexId);

            //添加到list集合
            arrayList.add(vertice);

            //创建图数据库节点
            System.out.println(id+"--"+vertexType+"--"+vertexId);
        }

        Integer sourceId = null;
        Integer targetId = null;
        String sourceVertexId = null;
        String targetVertexId = null;
        JSONArray edges = jsonObject.getJSONArray("edges");
        for (int i=0;i<edges.size();i++){
            JSONObject object = (JSONObject)edges.get(i);
            JSONArray sources = object.getJSONArray("sources");
            for (int y=0;y<sources.size();y++){
                sourceId = (Integer)sources.get(y);
            }
            JSONArray targets = object.getJSONArray("targets");
            for (int z=0;z<targets.size();z++){
                targetId = (Integer)targets.get(z);
            }
            String edgeType = object.getString("edgeType");

            //创建边
            if(edgeType.equals("PROJECTION")){
                for (vertice list:arrayList){
                    if (list.getId()==sourceId){
                        sourceVertexId = list.getVertexId();
                    }else if(list.getId()==targetId){
                        targetVertexId = list.getVertexId();
                    }
                }
                System.out.println(sourceVertexId+"--->"+targetVertexId);
                //创建边

            }
        }
    }
}
