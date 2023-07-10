package com.tencent.hr.datacenter.datacollect;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.dataSource.BusinessSourceNew;
import com.tencent.hr.datacenter.datacollect.dataSource.ProjectListSource_New;
import com.tencent.hr.datacenter.datacollect.dataSource.RelevancySourceNew;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
import com.tencent.hr.datacenter.datacollect.entity.RelevancyTableBean;
import com.tencent.hr.datacenter.datacollect.utils.JdbcUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import scala.reflect.internal.Trees;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ETCSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<BusinessProflile> streamB = env.addSource(new BusinessSourceNew());
        DataStreamSource<RelevancyTableBean> streamR = env.addSource(new RelevancySourceNew());
        DataStreamSource<ProjectListBean> streamP = env.addSource(new ProjectListSource_New());
        DataStreamSource<JdbcUtil.TM> streamE = env.addSource(new JdbcUtil());


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table tableB = tEnv.fromDataStream(streamB);
        Table tableR = tEnv.fromDataStream(streamR);
        Table tableP = tEnv.fromDataStream(streamP);
        Table tableE = tEnv.fromDataStream(streamE);

        tEnv.createTemporaryView("business", tableB);
        tEnv.createTemporaryView("relevancy", tableR);
        tEnv.createTemporaryView("projectlist", tableP);
        tEnv.createTemporaryView("Etc", tableE);

//        Table table = tEnv.sqlQuery("select * from Etc");
//        tEnv.toRetractStream(table,JdbcUtil.TM.class).print();


        //业务的写入
        Table result = tEnv.sqlQuery(
                Constant.FTEBusSql
        );
//        tEnv.createTemporaryView("bResult", result);


        DataStream<Tuple2<Boolean, BusinessProflile>> tuple2DataStream = tEnv.toRetractStream(result, BusinessProflile.class);
        SingleOutputStreamOperator<Tuple2<Boolean, BusinessProflile>> reduce = tuple2DataStream.
                keyBy(line -> line.f1.getId()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(1))).reduce((v1, v2) -> v2);
//        reduce.map(line->line.f1).print();

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
            }
        });

        //年度
        Table resultPro = tEnv.sqlQuery(
                "select pj.id , " +
                        "pj.workspace_id , " +
                        "pj.tworkflow_is_done , " +
                        "pj.tworkflow_id , " +
                        "pj.name , " +
                        "pj.description , " +
                        "pj.creator , " +
                        "pj.created , " +
                        "pj.modifier , " +
                        "pj.modified , " +
                        "pj.tworkflow_step_name , " +
                        "pj.tworkflow_statu_owner , " +
                        "pj.tworkflow_entry_id , " +
                        "pj.project_sort , " +
                        "pj.business_area , " +
                        "pj.business_segment , " +
                        "pj.project_cycle_finish_time , " +
                        "pj.project_cycle_start_time , " +
                        "pj.KR , " +
                        "pj.join_OKR , " +
                        "pj.new_this_year , " +
                        "pj.project_level , " +
                        "pj.stage_of_project , " +
                        "pj.project_progre_notes , " +
                        "pj.PM , " +
                        "pj.Sponsor , " +
                        "pj.execution_PM , " +
                        "pj.background , " +
                        "pj.business_goal , " +
                        "pj.solution , " +
                        "pj.metrics , " +
                        "pj.expected_risk , " +
                        "pj.project_member , " +
                        "pj.project_stakeholder , " +
                        "pj.resource_planning_operations , " +
                        "pj.resource_planning_develop , " +
                        "pj.resource_planning_outsourcing , " +
                        "pj.Document_Space , " +
                        "pj.project_progress_percentage , " +
                        "pj.rdp_id , " +
                        "pj.projects , " +
                        "pj.project_type , " +
                        "pj.proj_review_recommendations , " +
                        "pj.business_area_new , " +
                        "pj.resource_estimation , " +
                        "e.FTE_number as resource_real_consumption   ," +
                        "pj.workspace_name  " +
                        "from  projectlist as pj  " +
                        "join Etc as e on e.rioid = pj.id  "
        );
        DataStream<Tuple2<Boolean, ProjectListBean>> tuple2DataStreamPro = tEnv.toRetractStream(resultPro, ProjectListBean.class);
//        tuple2DataStreamPro.map(line->line.f1).print();
        SingleOutputStreamOperator<String> mapPro = tuple2DataStreamPro.map(new MapFunction<Tuple2<Boolean, ProjectListBean>, String>() {
            @Override
            public String map(Tuple2<Boolean, ProjectListBean> ctx) throws Exception {
                ProjectListBean f1 = ctx.f1;

                String eval = new hutoolTest().evalPro(f1.getId(), f1.getWorkspace_id(), f1.getTworkflow_is_done(), f1.getTworkflow_id(), f1.getName(), f1.getDescription(), f1.getCreator(),
                        f1.getCreated(), f1.getModifier(), f1.getModified(), f1.getTworkflow_step_name(), f1.getTworkflow_statu_owner(), f1.getTworkflow_entry_id(), f1.getProject_sort(),
                        f1.getBusiness_area(), f1.getBusiness_segment(), f1.getProject_cycle_finish_time(), f1.getProject_cycle_start_time(), f1.getKR(), f1.getJoin_OKR(), f1.getNew_this_year(),
                        f1.getProject_level(), f1.getStage_of_project(), f1.getProject_progre_notes(), f1.getPM(), f1.getSponsor(), f1.getExecution_PM(), f1.getBackground(), f1.getBusiness_goal(), f1.getSolution(),
                        f1.getMetrics(), f1.getExpected_risk(), f1.getProject_member(), f1.getProject_stakeholder(), f1.getResource_planning_operations(), f1.getResource_planning_develop(), f1.getResource_planning_outsourcing(),
                        f1.getDocument_Space(), f1.getProject_progress_percentage(), f1.getRdp_id(), f1.getProjects(), f1.getProject_type(), f1.getProj_review_recommendations(),
                        f1.getBusiness_area_new(), f1.getResource_estimation(), f1.getResource_real_consumption(), f1.getWorkspace_name()
                );
                return eval;
            }
        });


        env.execute();


    }

    //业务
    public static class hutoolTest {

        public String eval(String id, String workspace_id, String tworkflow_is_done, String tworkflow_id, String name, String description, String creator, String created, String modifier, String modified, String tworkflow_step_name, String tworkflow_statu_owner, String tworkflow_entry_id, String strategic_positioning, String core_functions, String specific_duties, String method_to_realize, String business_manager, String business_action, String business_value, String business_cost_FullTime, String business_cost_Inhouse, String business_cost_outsourced, String resource_planning_operations, String resource_planning_develop, String resource_planning_outsourcing, String business_ework_fulltime, String business_ework_inhouse, String business_ework_outsourced, String project_ework_operations, String project_ework_develop, String project_ework_outsourcing, String IT_CPU, String IT_memory, String IT_storage, String IT_DB, String IT_PAAS, String IT_pubilc, String level_group3, String business_project_outsourcing, String consulting_fees, String indirect_cost, String IT_public, String resource_estimation, String resource_real_consumption, String workspace_name) throws IOException {
            //正式环境
            int ID = Integer.parseInt(id);
            String ReUrl = String.format(Constant.BUSINESSURL, ID);
//            String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=10461";
            String auth = Constant.AUTH;
            String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));
            JSONObject data = new JSONObject();
            data.put("system_name", Constant.SYSTEM_NAEM_BUSINESS);
            data.put("id", id);
//            data.put("id", "10461");
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
//            data.put("modifier", "ceshi");
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
            data.put("resource_estimation",  resource_estimation == null ? null : new BigDecimal(resource_estimation).stripTrailingZeros().toPlainString());//resource_estimation
            data.put("resource_real_consumption", resource_real_consumption);
            data.put("workspace_name", workspace_name);

            String system_name = "business_profile_test";
            if (IsChangeResource_estimation(workspace_id, id, resource_estimation, resource_real_consumption, system_name, ReUrl)) {
//                if (id.equals("10457")) {
//                    System.out.println("10457+++++++++++++++++++");
//                if (id.equals("12055"))//真实环境拿其中一个业务来测试
                    PostHttpContent(ReUrl, encoding, data);
//                }
            }


//            System.out.println(id + "===" + name + "====" + resource_real_consumption + ":=======:" + data);

            return "sucess";

        }

        //年度
        public String evalPro(String id, String workspace_id, String tworkflow_is_done, String tworkflow_id, String name, String description, String creator, String created, String modifier, String modified, String tworkflow_step_name, String tworkflow_statu_owner, String tworkflow_entry_id, String project_sort, String business_area, String business_segment, String project_cycle_finish_time, String project_cycle_start_time, String KR, String join_OKR, String new_this_year, String project_level, String stage_of_project, String project_progre_notes, String PM, String Sponsor, String execution_PM, String background, String business_goal, String solution, String metrics, String expected_risk, String project_member, String project_stakeholder, String resource_planning_operations, String resource_planning_develop, String resource_planning_outsourcing, String Document_Space, String project_progress_percentage, String rdp_id, String projects, String project_type, String proj_review_recommendations, String business_area_new, String resource_estimation, String resource_real_consumption, String workspace_name) throws IOException {
            //正式环境
            int ID = Integer.parseInt(id);
            String ReUrl = String.format(Constant.PROFILE_URL, ID);
//            String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=10461";
            String auth = Constant.AUTH;
            String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));
            JSONObject data = new JSONObject();
            data.put("system_name", Constant.SYSTEM_NAME_PROFILE);
            data.put("id", id);
//            data.put("id", "10081");
            data.put("workspace_id", workspace_id);
//            data.put("workspace_id", 70025241);
            data.put("tworkflow_is_done", tworkflow_is_done);
            data.put("tworkflow_id", tworkflow_id);
            data.put("name", name);
            data.put("description", description);
            data.put("creator", creator);
            data.put("created", created);
//            data.put("modifier", modifier);
            data.put("modifier", "zhengshi");
//            data.put("modifier", "ceshi");
            data.put("modified", modified);
            data.put("tworkflow_step_name", tworkflow_step_name);
            data.put("tworkflow_statu_owner", tworkflow_statu_owner);
            data.put("tworkflow_entry_id", tworkflow_entry_id);
            data.put("project_sort", project_sort);
            data.put("business_area", business_area);
            data.put("business_segment", business_segment);
            data.put("project_cycle_finish_time", project_cycle_finish_time);
            data.put("project_cycle_start_time", project_cycle_start_time);
            data.put("KR", KR);
            data.put("join_OKR", join_OKR);
            data.put("new_this_year", new_this_year);
            data.put("project_level", project_level);
            data.put("stage_of_project", stage_of_project);
            data.put("project_progre_notes", project_progre_notes);
            data.put("PM", PM);
            data.put("Sponsor", Sponsor);
            data.put("execution_PM", execution_PM == null ? null : execution_PM.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("background", background == null ? null : background.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("business_goal", business_goal == null ? null : business_goal.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("solution", solution == null ? null : solution.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("metrics", metrics == null ? null : metrics.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("expected_risk", expected_risk == null ? null : expected_risk.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("project_member", project_member);
            data.put("project_stakeholder", project_stakeholder);
            data.put("resource_planning_operations", resource_planning_operations);
            data.put("resource_planning_develop", resource_planning_develop);
            data.put("resource_planning_outsourcing", resource_planning_outsourcing);
            data.put("Document_Space", Document_Space);
            data.put("project_progress_percentage", project_progress_percentage);
            data.put("rdp_id", rdp_id);
            data.put("projects", projects == null ? null : projects.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("project_type", project_type);
            data.put("proj_review_recommendations", proj_review_recommendations == null ? null : proj_review_recommendations.replaceAll("(------)+", "\n\n").replaceAll("---", "\n"));
            data.put("business_area_new", business_area_new);
            data.put("resource_estimation", resource_estimation);
            data.put("resource_real_consumption", resource_real_consumption == null ? null : new BigDecimal(resource_real_consumption).stripTrailingZeros().toPlainString());
            data.put("workspace_name", workspace_name);


            String system_name = "annual_projects_test";
            if (IsChangeResource_estimation(workspace_id, id, resource_estimation, resource_real_consumption, system_name, ReUrl)) {
//                if (id.equals("10090")||id.equals("10091")||id.equals("10092")||id.equals("10093"))
                    PostHttpContent(ReUrl, encoding, data);
            }
            return "sucess";

        }

    }

    //年度和业务的  资源配置比较 resource_estimation，如果没有改变的话，就不执行程序
    public static boolean IsChangeResource_estimation(String workspace_id, String id, String resource_estimation, String resource_real_consumption, String system_name, String reUrl) throws IOException {
        //测试环境
        String auth = Constant.AUTH;
        int ID = Integer.parseInt(id);
//        String ReUrl = String.format("http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=%d", ID);
        String ReUrl = reUrl;
        String result1 = GetHttpContent(ReUrl, auth);

        ObjectMapper objectMapper1 = new ObjectMapper();
        JsonNode jsonNode = objectMapper1.readTree(result1);
        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");
            for (JsonNode m : message) {
                JsonNode mm = m.get("TObjectInstance");

                String resourceEstimation = mm.get("resource_estimation").toString().replace("\"", "");
                String resourceRealConsumption = mm.get("resource_real_consumption").toString().replace("\"", "");

//                if (system_name.equals("annual_projects_test"))//年度
//                    return !(resourceEstimation.equals(resource_estimation) || (resourceEstimation == null) && (resource_estimation == null));
//                else if (system_name.equals("business_profile_test"))//业务
                return !(resourceRealConsumption.equals(resource_real_consumption) || (resourceRealConsumption == null) && (resource_real_consumption == null));
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
