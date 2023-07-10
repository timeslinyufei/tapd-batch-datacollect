package com.tencent.hr.datacenter.datacollect;


//import cn.hutool.http.HttpRequest;


import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.dataSource.BusinessSourceNew;
import com.tencent.hr.datacenter.datacollect.dataSource.ProjectListSource_New;
import com.tencent.hr.datacenter.datacollect.dataSource.RelevancySourceNew;
import com.tencent.hr.datacenter.datacollect.entity.*;
import com.tencent.hr.datacenter.flink.template.DataRow;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class DataSink {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<BusinessProflile> streamB = env.addSource(new BusinessSourceNew());
        DataStreamSource<RelevancyTableBean> streamR = env.addSource(new RelevancySourceNew());
        DataStreamSource<ProjectListBean> streamP = env.addSource(new ProjectListSource_New());


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table tableB = tEnv.fromDataStream(streamB);
        Table tableR = tEnv.fromDataStream(streamR);
        Table tableP = tEnv.fromDataStream(streamP);

        tEnv.createTemporaryView("business", tableB);
        tEnv.createTemporaryView("relevancy", tableR);
        tEnv.createTemporaryView("projectlist", tableP);

        Table result = tEnv.sqlQuery(
                Constant.RE_SQL
        );
        tEnv.createTemporaryView("bResult", result);


        DataStream<Tuple2<Boolean, BusinessProflile>> tuple2DataStream = tEnv.toRetractStream(result, BusinessProflile.class);

        SingleOutputStreamOperator<Tuple2<Boolean, BusinessProflile>> reduce = tuple2DataStream.
                keyBy(line -> line.f1.getId()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(1))).reduce((v1, v2) -> v2);

        SingleOutputStreamOperator<String> map = reduce.map(new MapFunction<Tuple2<Boolean, BusinessProflile>, String>() {
            @Override
            public String map(Tuple2<Boolean, BusinessProflile> ctx) throws Exception {
                BusinessProflile f1 = ctx.f1;

                String eval = new hutoolTest().eval(f1.getId(), f1.getWorkspace_id(), f1.getTworkflow_is_done(), f1.getTworkflow_id(),
                        f1.getName(), f1.getDescription(), f1.getCreator(), f1.getCreated(), f1.getModifier(), f1.getModified(),
                        f1.getTworkflow_step_name(), f1.getTworkflow_statu_owner(), f1.getTworkflow_entry_id(), f1.getStrategic_positioning(),
                        f1.getCore_functions(), f1.getSpecific_duties(), f1.getMethod_to_realize(), f1.getBusiness_manager(), f1.getBusiness_action(), f1.getBusiness_value(), f1.getBusiness_cost_FullTime(),
                        f1.getBusiness_cost_Inhouse(), f1.getBusiness_cost_outsourced(), f1.getResource_planning_operations(), f1.getResource_planning_develop(),
                        f1.getResource_planning_outsourcing(), f1.getBusiness_ework_fulltime(), f1.getBusiness_ework_inhouse(), f1.getBusiness_ework_outsourced(), f1.getProject_ework_operations(),
                        f1.getProject_ework_develop(), f1.getProject_ework_outsourcing(), f1.getIT_CPU(), f1.getIT_memory(), f1.getIT_storage(), f1.getIT_DB(), f1.getIT_PAAS(),
                        f1.getIT_pubilc(), f1.getLevel_group3(), f1.getBusiness_project_outsourcing(), f1.getConsulting_fees(), f1.getIndirect_cost(), f1.getIT_public(),
                        f1.getResource_estimation(), f1.getResource_real_consumption(), f1.getWorkspace_name());
                return eval;

//                return new HttpContent().init();
            }
        });
//        map.print();

        env.execute();


    }


    public static class hutoolTest {

        public String eval(String id, String workspace_id, String tworkflow_is_done, String tworkflow_id, String name, String description, String creator, String created, String modifier, String modified, String tworkflow_step_name, String tworkflow_statu_owner, String tworkflow_entry_id, String strategic_positioning, String core_functions, String specific_duties, String method_to_realize, String business_manager, String business_action, String business_value, String business_cost_FullTime, String business_cost_Inhouse, String business_cost_outsourced, String resource_planning_operations, String resource_planning_develop, String resource_planning_outsourcing, String business_ework_fulltime, String business_ework_inhouse, String business_ework_outsourced, String project_ework_operations, String project_ework_develop, String project_ework_outsourcing, String IT_CPU, String IT_memory, String IT_storage, String IT_DB, String IT_PAAS, String IT_pubilc, String level_group3, String business_project_outsourcing, String consulting_fees, String indirect_cost, String IT_public, String resource_estimation, String resource_real_consumption, String workspace_name) throws IOException {
            //正式环境
            int ID = Integer.parseInt(id);
//            String ReUrl = String.format("http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=%d", ID);
            String ReUrl = String.format(Constant.BUSINESSURL, ID);
            String auth = Constant.AUTH;
            String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));
            JSONObject data = new JSONObject();
            data.put("system_name", Constant.SYSTEM_NAEM_BUSINESS);
            data.put("id", id);
            data.put("workspace_id", workspace_id);
//            data.put("workspace_id", 70025241);
            data.put("tworkflow_is_done", tworkflow_is_done);
            data.put("tworkflow_id", tworkflow_id);
            data.put("name", name == null ? null : name.replaceAll("---", "\n"));
            data.put("description", description == null ? null : description.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("creator", creator);
            data.put("created", created);
//            data.put("modifier", modifier);
            data.put("modifier", "zhengshi");
//            data.put("modifier", "ceshi");//测试环境写入的名字
            data.put("modified", modified);
            data.put("tworkflow_step_name", tworkflow_step_name);
            data.put("tworkflow_statu_owner", tworkflow_statu_owner);
            data.put("tworkflow_entry_id", tworkflow_entry_id);
            data.put("strategic_positioning", strategic_positioning);
            data.put("core_functions", core_functions);
            data.put("specific_duties", specific_duties);
            data.put("method_to_realize", method_to_realize);
            data.put("business_manager", business_manager);
            data.put("business_action", business_action);
            data.put("business_value", business_value);
            data.put("business_cost_FullTime", business_cost_FullTime);
            data.put("business_cost_Inhouse", business_cost_Inhouse);
            data.put("business_cost_outsourced", business_cost_outsourced);
            data.put("resource_planning_operations", resource_planning_operations);
            data.put("resource_planning_develop", resource_planning_develop);
            data.put("resource_planning_outsourcing", resource_planning_outsourcing);
            data.put("project_ework_operations", project_ework_operations);
            data.put("project_ework_develop", project_ework_develop);
            data.put("project_ework_outsourcing", project_ework_outsourcing);
            data.put("IT_CPU", IT_CPU);
            data.put("IT_memory", IT_memory);
            data.put("IT_storage", IT_storage);
            data.put("IT_DB", IT_DB);
            data.put("3level_group", level_group3);
            data.put("business_project_outsourcing", business_project_outsourcing);
            data.put("consulting_fees", consulting_fees);
            data.put("indirect_cost", indirect_cost);
            data.put("IT_public", IT_public);
            data.put("resource_estimation", resource_estimation == null ? null : new BigDecimal(resource_estimation).stripTrailingZeros().toPlainString());//resource_estimation
            data.put("resource_real_consumption", resource_real_consumption);
            data.put("workspace_name", workspace_name);

//            HttpRequest.post(url)
//                    .header("Authorization", "Basic " + encoding)
//                    .body(String.valueOf(data))
//                    .execute();

//            System.out.println(id.equals("11232")+"---"+resource_estimation+"-------"+level_group3);
//            if (id.equals("11232"))
//            PostHttpContent(url,encoding,data);
//            System.out.println(id);
            if (IsChangeResource_estimation(workspace_id, id, resource_estimation)) {
                if (id.equals("12055")) {
                    PostHttpContent(ReUrl, encoding, data);
//                    System.out.println("yes");
                }


            }
//            System.out.println(id+"==="+name+"===="+data);

            return "sucess";

        }

    }

    //年度和业务的  资源配置比较 resource_estimation，如果没有改变的话，就不执行程序
    public static boolean IsChangeResource_estimation(String workspace_id, String id, String resource_estimation) throws IOException {
        //测试环境
        String auth = Constant.AUTH;
        int ID = Integer.parseInt(id);
        String ReUrl = String.format(Constant.BUSINESSURL, ID);
        String result1 = GetHttpContent(ReUrl, auth);

        ObjectMapper objectMapper1 = new ObjectMapper();
        JsonNode jsonNode = objectMapper1.readTree(result1);
        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");
            for (JsonNode m : message) {
                JsonNode mm = m.get("TObjectInstance");
                String resourceEstimation = mm.get("resource_estimation").toString().replace("\"", "");

//                System.out.println(resourceEstimation.equals(resource_estimation) + "=======" + ((resource_estimation == null) && (resource_estimation == null)) + "=======" + (resource_estimation == null) + "=============" + resourceEstimation + "==============" + resource_estimation);
//                System.out.println(id + "============="+(resourceEstimation.equals(resource_estimation) || ((resource_estimation == null) && (resource_estimation == null))));
                return !(resourceEstimation.equals(resource_estimation) || ((resource_estimation == null) && (resource_estimation == null)));
            }

        }
        return true;
    }

    public static String GetHttpContent(String url, String auth) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));

        httpGet.setHeader("Authorization", "Basic " + encoding);

        CloseableHttpResponse response = httpClient.execute(httpGet);
        int responseCode = response.getStatusLine().getStatusCode();
        if (responseCode == 200) {
            HttpEntity entity = response.getEntity();
            entity.getContent();

            InputStream input = entity.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            String str1 = br.readLine();
            String result1 = new String(str1.getBytes(StandardCharsets.US_ASCII), StandardCharsets.UTF_8);
            br.close();
            input.close();
            return result1;


        }
        return "";
    }

    public static void PostHttpContent(String url, String encoding, JSONObject data) throws IOException {

        try {

            URL obj = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Connection", "Keep-Alive");// 维持长连接
            connection.setRequestProperty("Charset", "UTF-8");
            connection.setRequestProperty("accept", "*/*");
            String authHeaderValue = "Basic " + new String(encoding);
            connection.setRequestProperty("Authorization", authHeaderValue);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            //建立输入流，向指向的URL传入参数
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            out.write(data.toJSONString().getBytes(StandardCharsets.UTF_8));
            out.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));//,"UTF-8"
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {

                response.append(inputLine);
            }


            in.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}