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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
//import org.postgresql.Driver;


import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
***这个是每次执行。做一次本地备份的代码
 */
public class Backups {


    public static void main(String[] args) throws Exception {
        //TODO 备份
//        /*
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        DataStreamSource<ProjectListBean> streamP = env.addSource(new ProjectListSource_New());
//        DataStreamSource<RelevancyTableBean> streamR = env.addSource(new RelevancySourceNew());
//
//
//        streamR.map(line->line).print();

//        DataStreamSource<JdbcUtil.TM> tmDataStreamSource = env.addSource(new JdbcUtil());
        //TODO 备份
        DataStreamSource<BusinessProflile> businessProflileDataStreamSource = env.addSource(new BusinessSourceNew());
        DataStreamSource<ProjectListBean> projectListBeanDataStreamSource = env.addSource(new ProjectListSource_New());

//        DataStreamSource<RelevancyTableBean> relevancyTableBeanDataStreamSource = env.addSource(new RelevancySourceNew());

//        tmDataStreamSource.map(line->line).print();


        //TODO 读取备份数据

//        /*备份数据
        //业务

        SingleOutputStreamOperator<String> bus = businessProflileDataStreamSource.map(new MapFunction<BusinessProflile, String>() {


            @Override
            public String map(BusinessProflile businessProflile) throws Exception {


                return businessProflile.toString();
            }
        });
        String outputPath = "D:\\output\\business.txt";


        // 写入数据到本地文件
        bus.writeAsText(outputPath);


        //年度
        SingleOutputStreamOperator<ProjectListBean> pro = projectListBeanDataStreamSource.map(new MapFunction<ProjectListBean, ProjectListBean>() {

            @Override
            public ProjectListBean map(ProjectListBean projectListBean) throws Exception {
                return projectListBean;
            }
        });
        String outputPath1 = "D:\\output\\project.txt";


        // 写入数据到本地文件
        pro.writeAsText(outputPath1);
//*/


        //TODO 回填到tapd
        /*

//        1.拿到批处理的执行环境
        ExecutionEnvironment envE = ExecutionEnvironment.getExecutionEnvironment();
        envE.setParallelism(1);
//        2.读取数据
//        DataSource<String> dataSource = envE.readTextFile("D:\\output\\projectSourceNew.txt");
        DataSource<String> dataSourceBus = envE.readTextFile("D:\\output\\business.txt");


        //TODO 业务回填
//        /*
        dataSourceBus.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {

                String input = s;

                // 创建 ProjectListBean 对象
                BusinessProflile f1 = new BusinessProflile();

                // 使用字符串拆分的方式获取每个属性的键值对并设置到对象中

//                System.out.println(input);

                String[] keyValuePairs = input.replaceAll("BusinessProflile", "").replaceAll("[()]", "").split(", ");

                for (String keyValuePair : keyValuePairs) {
                    String[] keyValue = keyValuePair.split("=");
                    String key = keyValue[0];
                    String value = keyValue.length == 1 ? null : keyValue[1];
//                    System.out.println(key);
                    setValueBus(f1, key, value);
                }

                String eval = new hutoolTest().eval(f1.getId(), f1.getWorkspace_id(), f1.getTworkflow_is_done(), f1.getTworkflow_id(),
                        f1.getName(), f1.getDescription(), f1.getCreator(), f1.getCreated(), f1.getModifier(), f1.getModified(),
                        f1.getTworkflow_step_name(), f1.getTworkflow_statu_owner(), f1.getTworkflow_entry_id(), f1.getStrategic_positioning(),
                        f1.getCore_functions(), f1.getSpecific_duties(), f1.getMethod_to_realize(), f1.getBusiness_manager(), f1.getBusiness_action(), f1.getBusiness_value(), f1.getBusiness_cost_FullTime(),
                        f1.getBusiness_cost_Inhouse(), f1.getBusiness_cost_outsourced(), f1.getResource_planning_operations(), f1.getResource_planning_develop(),
                        f1.getResource_planning_outsourcing(), f1.getBusiness_ework_fulltime(), f1.getBusiness_ework_inhouse(), f1.getBusiness_ework_outsourced(), f1.getProject_ework_operations(),
                        f1.getProject_ework_develop(), f1.getProject_ework_outsourcing(), f1.getIT_CPU(), f1.getIT_memory(), f1.getIT_storage(), f1.getIT_DB(), f1.getIT_PAAS(),
                        f1.getIT_pubilc(), f1.getLevel_group3(), f1.getBusiness_project_outsourcing(), f1.getConsulting_fees(), f1.getIndirect_cost(), f1.getIT_public(),
                        f1.getResource_estimation(), f1.getResource_real_consumption(), f1.getWorkspace_name());
                return "";

            }
        }).print();

         */


        //TODO 年度回填
        /*
        dataSource.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {

                String input = s;

                // 创建 ProjectListBean 对象
                ProjectListBean projectListBean = new ProjectListBean();

                // 使用字符串拆分的方式获取每个属性的键值对并设置到对象中
                String[] keyValuePairs = input.replaceAll("ProjectListBean", "").replaceAll("[()]", "").split(", ");
                for (String keyValuePair : keyValuePairs) {
                    String[] keyValue = keyValuePair.split("=");
                    String key = keyValue[0];
                    String value = keyValue.length == 1 ? "": keyValue[1];
                    setValue(projectListBean, key, value);
                }

                String eval = new hutoolTest().evalPro(projectListBean.getId(), projectListBean.getWorkspace_id(), projectListBean.getTworkflow_is_done(), projectListBean.getTworkflow_id(), projectListBean.getName(), projectListBean.getDescription(), projectListBean.getCreator(),
                        projectListBean.getCreated(), projectListBean.getModifier(), projectListBean.getModified(), projectListBean.getTworkflow_step_name(), projectListBean.getTworkflow_statu_owner(), projectListBean.getTworkflow_entry_id(), projectListBean.getProject_sort(),
                        projectListBean.getBusiness_area(), projectListBean.getBusiness_segment(), projectListBean.getProject_cycle_finish_time(), projectListBean.getProject_cycle_start_time(), projectListBean.getKR(), projectListBean.getJoin_OKR(), projectListBean.getNew_this_year(),
                        projectListBean.getProject_level(), projectListBean.getStage_of_project(), projectListBean.getProject_progre_notes(), projectListBean.getPM(), projectListBean.getSponsor(), projectListBean.getExecution_PM(), projectListBean.getBackground(), projectListBean.getBusiness_goal(), projectListBean.getSolution(),
                        projectListBean.getMetrics(), projectListBean.getExpected_risk(), projectListBean.getProject_member(), projectListBean.getProject_stakeholder(), projectListBean.getResource_planning_operations(), projectListBean.getResource_planning_develop(), projectListBean.getResource_planning_outsourcing(),
                        projectListBean.getDocument_Space(), projectListBean.getProject_progress_percentage(), projectListBean.getRdp_id(), projectListBean.getProjects(), projectListBean.getProject_type(), projectListBean.getProj_review_recommendations(),
                        projectListBean.getBusiness_area_new(), projectListBean.getResource_estimation(), projectListBean.getResource_real_consumption(), projectListBean.getWorkspace_name()
                );
                return eval;

            }
        }).print();


         */

//        envE.execute();


        //TODO 备份
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
//            data.put("workspace_id", "70025241");
            data.put("tworkflow_is_done", tworkflow_is_done);
            data.put("tworkflow_id", tworkflow_id);
            data.put("name", name.replaceAll("---","\n"));
            data.put("description", description.replaceAll("---","\n"));
            data.put("creator", creator);
            data.put("created", created);
//            data.put("modifier", modifier);
            data.put("modifier", "ceshi");
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
            data.put("resource_estimation", resource_estimation);
            data.put("resource_real_consumption", resource_real_consumption);
            data.put("workspace_name", workspace_name);
//            data.put("workspace_name", "SDC业务档案(测试)");

            String system_name = "business_profile_test";
            if (id.equals("10457")) {
                System.out.println(data);
                System.out.println("========10457================");
                System.out.println(name);
                PostHttpContent(ReUrl, encoding, data);

            }


            return "sucess";

        }

        //年度
        public String evalPro(String id, String workspace_id, String tworkflow_is_done, String tworkflow_id, String name, String description, String creator, String created, String modifier, String modified, String tworkflow_step_name, String tworkflow_statu_owner, String tworkflow_entry_id, String project_sort, String business_area, String business_segment, String project_cycle_finish_time, String project_cycle_start_time, String KR, String join_OKR, String new_this_year, String project_level, String stage_of_project, String project_progre_notes, String PM, String Sponsor, String execution_PM, String background, String business_goal, String solution, String metrics, String expected_risk, String project_member, String project_stakeholder, String resource_planning_operations, String resource_planning_develop, String resource_planning_outsourcing, String Document_Space, String project_progress_percentage, String rdp_id, String projects, String project_type, String proj_review_recommendations, String business_area_new, String resource_estimation, String resource_real_consumption, String workspace_name) throws IOException {

            //正式环境
//            int ID = Integer.parseInt(id);
            int ID = Integer.parseInt("10051");
            String ReUrl = String.format(Constant.PROFILE_URL, ID);
//            String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=10461";
            String auth = Constant.AUTH;
            String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));
            JSONObject data = new JSONObject();
            data.put("system_name", Constant.SYSTEM_NAME_PROFILE);
//            data.put("id", id);
            data.put("id", "10051");
//            data.put("workspace_id", workspace_id);
            data.put("workspace_id", 70025241);
            data.put("tworkflow_is_done", tworkflow_is_done);
            data.put("tworkflow_id", tworkflow_id);
//            data.put("name", name);
            data.put("name", "0424项目test");
            data.put("description", description);
            data.put("creator", creator);
            data.put("created", created);
//            data.put("modifier", modifier);
            data.put("modifier", "ceshi");
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
            data.put("execution_PM", execution_PM.replaceAll("---","\n"));
            data.put("background", background.replaceAll("---","\n"));
            data.put("business_goal", business_goal.replaceAll("---","\n"));
            data.put("solution", solution.replaceAll("---","\n"));
            data.put("metrics", metrics.replaceAll("---","\n"));
            data.put("expected_risk", expected_risk.replaceAll("---","\n"));
            data.put("project_member", project_member);
            data.put("project_stakeholder", project_stakeholder);
            data.put("resource_planning_operations", resource_planning_operations);
            data.put("resource_planning_develop", resource_planning_develop);
            data.put("resource_planning_outsourcing", resource_planning_outsourcing);
            data.put("Document_Space", Document_Space);
            data.put("project_progress_percentage", project_progress_percentage);
            data.put("rdp_id", rdp_id);
            data.put("projects", projects.replaceAll("---","\n"));
            data.put("project_type", project_type);
            data.put("proj_review_recommendations", proj_review_recommendations.replaceAll("---","\n"));
            data.put("business_area_new", business_area_new);
            data.put("resource_estimation", resource_estimation);
            data.put("resource_real_consumption", resource_real_consumption);
            data.put("workspace_name", workspace_name);


            String system_name = "annual_projects_test";
            System.out.println("id.equals(\"10296\")=========" + id.equals("10044"));
            if (id.equals("10044"))
                PostHttpContent(ReUrl, encoding, data);
            return "sucess";

        }

    }


    //年度设置值
    private static void setValue(ProjectListBean obj, String key, String value) {
        if (value.equals("null")) value = null;
        switch (key) {
            case "id":
                obj.setId(value);
                break;
            case "workspace_id":
                obj.setWorkspace_id(value);
                break;
            case "tworkflow_is_done":
                obj.setTworkflow_is_done(value);
                break;
            case "tworkflow_id":
                obj.setTworkflow_id(value);
                break;
            case "name":
                obj.setName(value);
                break;
            case "description":
                obj.setDescription(value);
                break;
            case "creator":
                obj.setCreator(value);
                break;
            case "created":
                obj.setCreated(value);
                break;
            case "modifier":
                obj.setModifier(value);
                break;
            case "modified":
                obj.setModified(value);
                break;
            case "tworkflow_step_name":
                obj.setTworkflow_step_name(value);
                break;
            case "tworkflow_statu_owner":
                obj.setTworkflow_statu_owner(value);
                break;
            case "tworkflow_entry_id":
                obj.setTworkflow_entry_id(value);
                break;
            case "project_sort":
                obj.setProject_sort(value);
                break;
            case "business_area":
                obj.setBusiness_area(value);
                break;
            case "business_segment":
                obj.setBusiness_segment(value);
                break;

            case "project_cycle_finish_time":
                obj.setProject_cycle_finish_time(value);
                break;
            case "project_cycle_start_time":
                obj.setProject_cycle_start_time(value);
                break;
            case "KR":
                obj.setKR(value);
                break;
            case "join_OKR":
                obj.setJoin_OKR(value);
                break;
            case "new_this_year":
                obj.setNew_this_year(value);
                break;
            case "project_level":
                obj.setProject_level(value);
                break;
            case "stage_of_project":
                obj.setStage_of_project(value);
                break;
            case "project_progre_notes":
                obj.setProject_progre_notes(value);
                break;
            case "PM":
                obj.setPM(value);
                break;
            case "Sponsor":
                obj.setSponsor(value);
                break;
            case "execution_PM":
                obj.setExecution_PM(value);
                break;
            case "background":
                obj.setBackground(value);
                break;
            case "business_goal":
                obj.setBusiness_goal(value);
                break;
            case "solution":
                obj.setSolution(value);
                break;
            case "metrics":
                obj.setMetrics(value);
                break;
            case "expected_risk":
                obj.setExpected_risk(value);
                break;

            case "project_member":
                obj.setProject_member(value);
                break;
            case "project_stakeholder":
                obj.setProject_stakeholder(value);
                break;
            case "resource_planning_operations":
                obj.setResource_planning_operations(value);
                break;
            case "resource_planning_develop":
                obj.setResource_planning_develop(value);
                break;
            case "resource_planning_outsourcing":
                obj.setResource_planning_outsourcing(value);
                break;
            case "project_progress_percentage":
                obj.setProject_progress_percentage(value);
                break;
            case "rdp_id":
                obj.setRdp_id(value);
                break;

            case "projects":
                obj.setProjects(value);
                break;
            case "project_type":
                obj.setProject_type(value);
                break;
            case "proj_review_recommendations":
                obj.setProj_review_recommendations(value);
                break;
            case "business_area_new":
                obj.setBusiness_area_new(value);
                break;
            case "resource_estimation":
                obj.setResource_estimation(value);
                break;
            case "resource_real_consumption":
                obj.setResource_real_consumption(value);
                break;
            case "workspace_name":
                obj.setWorkspace_name(value);
                break;
        }

    }

    //业务设置值
    private static void setValueBus(BusinessProflile obj, String key, String value) {
        if (value.equals("null")) value = null;
        switch (key) {
            case "id":
                obj.setId(value);
                break;
            case "workspace_id":
                obj.setWorkspace_id(value);
                break;
            case "tworkflow_is_done":
                obj.setTworkflow_is_done(value);
                break;
            case "tworkflow_id":
                obj.setTworkflow_id(value);
                break;
            case "name":
                obj.setName(value);
                break;
            case "description":
                obj.setDescription(value);
                break;
            case "creator":
                obj.setCreator(value);
                break;
            case "created":
                obj.setCreated(value);
                break;
            case "modifier":
                obj.setModifier(value);
                break;
            case "modified":
                obj.setModified(value);
                break;
            case "tworkflow_step_name":
                obj.setTworkflow_step_name(value);
                break;
            case "tworkflow_statu_owner":
                obj.setTworkflow_statu_owner(value);
                break;
            case "tworkflow_entry_id":
                obj.setTworkflow_entry_id(value);
                break;
            case "strategic_positioning":
                obj.setStrategic_positioning(value);
                break;
            case "core_functions":
                obj.setCore_functions(value);
                break;
            case "specific_duties":
                obj.setSpecific_duties(value);
                break;

            case "method_to_realize":
                obj.setMethod_to_realize(value);
                break;
            case "business_manager":
                obj.setBusiness_manager(value);
                break;
            case "business_action":
                obj.setBusiness_action(value);
                break;
            case "business_value":
                obj.setBusiness_value(value);
                break;
            case "business_cost_FullTime":
                obj.setBusiness_cost_FullTime(value);
                break;
            case "business_cost_Inhouse":
                obj.setBusiness_cost_Inhouse(value);
                break;
            case "business_cost_outsourced":
                obj.setBusiness_cost_outsourced(value);
                break;
            case "resource_planning_operations":
                obj.setResource_planning_operations(value);
                break;
            case "resource_planning_develop":
                obj.setResource_planning_develop(value);
                break;
            case "resource_planning_outsourcing":
                obj.setResource_planning_outsourcing(value);
                break;
            case "business_ework_fulltime":
                obj.setBusiness_ework_fulltime(value);
                break;
            case "business_ework_inhouse":
                obj.setBusiness_ework_inhouse(value);
                break;
            case "business_ework_outsourced":
                obj.setBusiness_ework_outsourced(value);
                break;
            case "project_ework_operations":
                obj.setProject_ework_operations(value);
                break;
            case "project_ework_develop":
                obj.setProject_ework_develop(value);
                break;
            case "project_ework_outsourcing":
                obj.setProject_ework_outsourcing(value);
                break;

            case "IT_CPU":
                obj.setIT_CPU(value);
                break;
            case "IT_memory":
                obj.setIT_memory(value);
                break;
            case "IT_storage":
                obj.setIT_storage(value);
                break;
            case "IT_DB":
                obj.setIT_DB(value);
                break;
            case "IT_PAAS":
                obj.setIT_PAAS(value);
                break;
            case "IT_pubilc":
                obj.setIT_pubilc(value);
                break;
            case "level_group3":
                obj.setLevel_group3(value);
                break;

            case "business_project_outsourcing":
                obj.setBusiness_project_outsourcing(value);
                break;
            case "consulting_fees":
                obj.setConsulting_fees(value);
                break;
            case "indirect_cost":
                obj.setIndirect_cost(value);
                break;
            case "IT_public":
                obj.setIT_public(value);
                break;
            case "resource_estimation":
                obj.setResource_estimation(value);
                break;
            case "resource_real_consumption":
                obj.setResource_real_consumption(value);
                break;
            case "workspace_name":
                obj.setWorkspace_name(value);
                break;
        }

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
