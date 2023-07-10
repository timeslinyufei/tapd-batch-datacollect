package com.tencent.hr.datacenter.datacollect;

import com.alibaba.fastjson.JSONObject;
//import com.tencent.hr.datacenter.datacollect.entity.HrCompanyEntity;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
import com.tencent.hr.datacenter.datacollect.entity.RelevancyTableBean;
import com.tencent.hr.datacenter.flink.template.DataRow;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class TestMain {


    //业务档案数据(查询年度id匹配关系) 测试
    /*
    public static void main(String[] args) throws Exception, NumberFormatException {


        //TODO 测试
        //业务数据连接
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条，需要做一个append
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";//测试数据200条

        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";


        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        ObjectMapper objectMapper = new ObjectMapper();


        String result1 = GetHttpContent(url, auth);

        String result2 = GetHttpContent(url2, auth);//第二页的数据

        JsonNode jsonNode = objectMapper.readTree(result1);
        JsonNode jsonNode22 = objectMapper.readTree(result2);

        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");
            List<BusinessProflile> list1 = new ArrayList<>();
            ArrayList<String> arrayList = new ArrayList<>();


            for (JsonNode m : message) {
                String content = m.get("TObjectInstance").toString();
                arrayList.add(content);
            }
            if (jsonNode22.has("data")) {
                JsonNode m2 = jsonNode.get("data");
                for (JsonNode m : m2) {
                    String content = m.get("TObjectInstance").toString();
                    arrayList.add(content);
                }
            }

            list1 = JSONObject.parseArray(arrayList.toString(), BusinessProflile.class);

            if (CollectionUtils.isNotEmpty(list1)) {
                for (BusinessProflile entity1 : list1) {
                    int tobject_instance_id = Integer.parseInt(entity1.getId());
                    String ReUrl = String.format("http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?workspace_id=69993035&system_name=business_list&rel_system_name=project_list&tobject_instance_id=%d", tobject_instance_id);
                    String reStr = GetHttpContent(ReUrl, auth);
                    JsonNode jsonNode2 = objectMapper.readTree(reStr);
                    if (jsonNode.has("data")) {
                        JsonNode reProjectId = jsonNode2.get("data").findValue("id");//依赖的年度项目id
                        if (reProjectId != null) {

                            DataRow dataRow = new DataRow(2);
                            dataRow.setField(0, tobject_instance_id);
                            dataRow.setField(1, reProjectId.asInt());
                            dataRow.setAssigner(null);
                            System.out.println(dataRow);
                        }
                    }
                }
            }
        }

    }

     */

    private static String GetHttpContent(String url, String auth) throws IOException {
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


    //年度数据测试
/*
    public static void main(String[] args) throws Exception {


        //TODO 测试
        //年度项目数据
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200";
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200";//测试数据3条

        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        int interval = 1000;
        int maxAttempts = 1;
        ObjectMapper objectMapper = new ObjectMapper();


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
            JsonNode jsonNode = objectMapper.readTree(result1);
            if (jsonNode.has("data")) {
                JsonNode message = jsonNode.get("data");

                List<ProjectListBean> list1 = new ArrayList<>();
                ArrayList<String> arrayList = new ArrayList<>();
                for (JsonNode m : message) {
                    String content = m.get("TObjectInstance").toString();
                    arrayList.add(content);
//                    System.out.println(content);
                }

                list1 = JSONObject.parseArray(arrayList.toString(), ProjectListBean.class);


                if (CollectionUtils.isNotEmpty(list1)) {
                    for (ProjectListBean entity1 : list1) {
                        DataRow dataRow = new DataRow(46);
                        dataRow.setField(0, entity1.getId());
                        dataRow.setField(1, entity1.getWorkspace_id());
                        dataRow.setField(2, entity1.getTworkflow_is_done());
                        dataRow.setField(3, entity1.getTworkflow_id());
                        dataRow.setField(4, entity1.getName());
                        dataRow.setField(5, entity1.getDescription());
                        dataRow.setField(6, entity1.getCreated());
                        dataRow.setField(7, entity1.getModifier());
                        dataRow.setField(8, entity1.getModified());
                        dataRow.setField(9, entity1.getTworkflow_step_name());
                        dataRow.setField(10, entity1.getTworkflow_statu_owner());
                        dataRow.setField(11, entity1.getTworkflow_entry_id());
                        dataRow.setField(12, entity1.getProject_sort());
                        dataRow.setField(13, entity1.getBusiness_area());
                        dataRow.setField(14, entity1.getBusiness_segment());
                        dataRow.setField(15, entity1.getProject_cycle_finish_time());
                        dataRow.setField(16, entity1.getProject_cycle_start_time());
                        dataRow.setField(17, entity1.getKR());
                        dataRow.setField(18, entity1.getJoin_OKR());
                        dataRow.setField(19, entity1.getNew_this_year());
                        dataRow.setField(20, entity1.getProject_level());
                        dataRow.setField(21, entity1.getStage_of_project());

                        dataRow.setField(22, entity1.getProject_progre_notes());
                        dataRow.setField(23, entity1.getPM());
                        dataRow.setField(24, entity1.getSponsor());
                        dataRow.setField(25, entity1.getExecution_PM());
                        dataRow.setField(26, entity1.getBackground());
                        dataRow.setField(27, (entity1.getBusiness_goal() == null ? entity1.getBusiness_goal() : entity1.getBusiness_goal().replaceAll("\\n", "---")));
                        dataRow.setField(28, entity1.getSolution());
                        dataRow.setField(29, entity1.getMetrics());
                        dataRow.setField(30, entity1.getExpected_risk());
                        dataRow.setField(31, entity1.getProject_member());
                        dataRow.setField(32, entity1.getProject_stakeholder());
                        dataRow.setField(33, entity1.getResource_planning_operations());
                        dataRow.setField(34, entity1.getResource_planning_develop());
                        dataRow.setField(35, entity1.getResource_planning_outsourcing());
                        dataRow.setField(36, entity1.getDocument_Space());
                        dataRow.setField(37, entity1.getProject_progress_percentage());
                        dataRow.setField(38, entity1.getRdp_id());
                        dataRow.setField(39, entity1.getProjects());
                        dataRow.setField(40, entity1.getProject_type());
                        dataRow.setField(41, entity1.getProj_review_recommendations());
                        dataRow.setField(42, entity1.getBusiness_area_new());
                        dataRow.setField(43, entity1.getResource_estimation());
                        dataRow.setField(44, entity1.getResource_real_consumption());
                        dataRow.setField(45, entity1.getWorkspace_name());
                        dataRow.setAssigner(null);
//                        System.out.println(dataRow);
//                        System.out.println("1===="+(entity1.getBackground() == null ?entity1.getBackground():entity1.getBackground().replaceAll("(\r|\n)+", "---")));
//                        System.out.println(entity1.getRdp_id());
//                        System.out.println("1===="+(entity1.getBusiness_goal() == null ? entity1.getBusiness_goal() : entity1.getBusiness_goal().replaceAll("(\r|\n)+", "---")));
//                        System.out.println(entity1.getBusiness_goal().replaceAll("\\n", "---"));
                    }
                } else {
                    System.exit(-1);        //为空则异常退出
                }


            }
            br.close();
            input.close();
        }

    }




 */
//业务数据（v1.0）
/*
//    业务数据
    public static void main(String[] args) throws Exception {
//        String appid="hrdw";
//        String appsecret="30965827-6FF9-AC1B-8D50-ABDA7554F88E";
//        String url="http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035";
//        String nonce= UUID.randomUUID().toString().toUpperCase();
//        HttpUtil httpUtil = new HttpUtil();
//        String timestamp=String.valueOf(System.currentTimeMillis()/1000);
//        String params=String.format("appid=%s&nonce=%s&timestamp=%s",appid,nonce,timestamp);
//        String strSignatureTemp=String.format("%s&key=%s",params,appsecret);
//        String signature= Utils.encrypt(strSignatureTemp,appsecret,"HmacSHA256").toUpperCase();
//        String requestUrl = String.format("%s?%s&signature=%s",
//                url,params,signature);
//        String result = httpUtil.getAndRetry(requestUrl,3);
//        List<HrCompanyEntity> list = new ArrayList<>();
//        list = JSONObject.parseArray(result, HrCompanyEntity.class);
//            for(HrCompanyEntity entity:list){
//                System.out.println(entity.toString());
//                if(Utils.checkNull(entity.getLocateCity())!=null) {
//                    System.out.println(entity.getLocateCity());
//                }else{
//                    System.out.println("xxx:"+entity.getLocateCity());
//                }
//            }

        //TODO 测试
        //业务档案数据
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条。接口只能一次访问200条数据，需要将数据结合
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        int interval = 1000;
        int maxAttempts = 1;
        ObjectMapper objectMapper = new ObjectMapper();


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
            JsonNode jsonNode = objectMapper.readTree(result1);
            if (jsonNode.has("data")) {
                JsonNode message = jsonNode.get("data");

                List<BusinessProflile> list1 = new ArrayList<>();
                ArrayList<String> arrayList = new ArrayList<>();
                for (JsonNode m : message) {
                    String content = m.get("TObjectInstance").toString();
                    arrayList.add(content);
                }
                list1 = JSONObject.parseArray(arrayList.toString(), BusinessProflile.class);

                if (CollectionUtils.isNotEmpty(list1)) {
                    for (BusinessProflile entity1 : list1) {
                        DataRow dataRow = new DataRow(38);
                        dataRow.setField(0, entity1.getId());
                        dataRow.setField(1, entity1.getWorkspace_id());
                        dataRow.setField(2, entity1.getTworkflow_is_done());
                        dataRow.setField(3, entity1.getTworkflow_id());
                        dataRow.setField(4, entity1.getName());
                        dataRow.setField(5, entity1.getDescription());
                        dataRow.setField(6, entity1.getCreated());
                        dataRow.setField(7, entity1.getModifier());
                        dataRow.setField(8, entity1.getModified());
                        dataRow.setField(9, entity1.getTworkflow_step_name());
                        dataRow.setField(10, entity1.getTworkflow_statu_owner());
                        dataRow.setField(11, entity1.getTworkflow_entry_id());
                        dataRow.setField(12, entity1.getStrategic_positioning());
                        dataRow.setField(13, entity1.getCore_functions());
                        dataRow.setField(14, entity1.getSpecific_duties());
                        dataRow.setField(15, entity1.getMethod_to_realize());
                        dataRow.setField(16, entity1.getBusiness_manager());
                        dataRow.setField(17, entity1.getBusiness_action());
                        dataRow.setField(18, entity1.getBusiness_value());
                        dataRow.setField(19, entity1.getBusiness_cost_FullTime());
                        dataRow.setField(20, entity1.getBusiness_cost_Inhouse());
                        dataRow.setField(21, entity1.getBusiness_cost_outsourced());
                        dataRow.setField(22, entity1.getResource_planning_operations());
                        dataRow.setField(23, entity1.getResource_planning_develop());
                        dataRow.setField(24, entity1.getResource_planning_outsourcing());
                        dataRow.setField(25, entity1.getProject_ework_operations());
                        dataRow.setField(26, entity1.getProject_ework_develop());
                        dataRow.setField(27, entity1.getProject_ework_outsourcing());
                        dataRow.setField(28, entity1.getIT_CPU());
                        dataRow.setField(29, entity1.getIT_memory());
                        dataRow.setField(30, entity1.getIT_storage());
                        dataRow.setField(31, entity1.getIT_DB());
                        dataRow.setField(32, entity1.getIT_PAAS());
                        dataRow.setField(33, entity1.getIT_pubilc());
                        dataRow.setField(34, entity1.getLevel_group3());
                        dataRow.setField(35, entity1.getResource_estimation());
                        dataRow.setField(36, entity1.getResource_real_consumption());
                        dataRow.setField(37, entity1.getWorkspace_name());
                        dataRow.setAssigner(null);
                        System.out.println(dataRow);

                    }
                } else {
                    System.exit(-1);        //为空则异常退出
                }

            }
            br.close();
            input.close();
        }

    }

 */


    //业务数据（v2.0)
    /*
    public static void main(String[] args) throws Exception, NumberFormatException {
        //业务档案数据
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条。接口只能一次访问200条数据，需要将数据结合
        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        ObjectMapper objectMapper1 = new ObjectMapper();
        ObjectMapper objectMapper2 = new ObjectMapper();


        String result1 = GetHttpContent(url, auth);

        String result2 = GetHttpContent(url2, auth);//第二页的数据


        JsonNode jsonNode = objectMapper1.readTree(result1);
        JsonNode jsonNode2 = objectMapper2.readTree(result2);
        List<BusinessProflile> list1 = new ArrayList<>();

        ArrayList<String> arrayList = new ArrayList<>();

        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");


            for (JsonNode m : message) {
                String content = m.get("TObjectInstance").toString();
                arrayList.add(content);
                System.out.println("1=========" + content);
            }
        }
        if (jsonNode2.has("data")) {
            JsonNode m2 = jsonNode2.get("data");
            for (JsonNode m : m2) {
                String content1 = m.get("TObjectInstance").toString();
                arrayList.add(content1);
                System.out.println("2=========" + content1);

            }
        }


        list1 = JSONObject.parseArray(arrayList.toString(), BusinessProflile.class);

        if (CollectionUtils.isNotEmpty(list1)) {
            for (BusinessProflile entity1 : list1) {
                DataRow dataRow = new DataRow(38);
                dataRow.setField(0, entity1.getId());
                dataRow.setField(1, entity1.getWorkspace_id());
                dataRow.setField(2, entity1.getTworkflow_is_done());
                dataRow.setField(3, entity1.getTworkflow_id());
                dataRow.setField(4, entity1.getName() == null ? entity1.getName() : entity1.getName().replaceAll("(\r|\n)+", ""));
//                        dataRow.setField(5, entity1.getDescription());
                dataRow.setField(5, (entity1.getDescription() == null ? entity1.getDescription() : entity1.getDescription().replaceAll("(\r|\n)+", "---")));
                dataRow.setField(6, entity1.getCreated());
                dataRow.setField(7, entity1.getModifier());
                dataRow.setField(8, entity1.getModified());
                dataRow.setField(9, entity1.getTworkflow_step_name());
                dataRow.setField(10, entity1.getTworkflow_statu_owner());
                dataRow.setField(11, entity1.getTworkflow_entry_id());
                dataRow.setField(12, entity1.getStrategic_positioning());
                dataRow.setField(13, entity1.getCore_functions());
                dataRow.setField(14, entity1.getSpecific_duties());
                dataRow.setField(15, entity1.getMethod_to_realize());
                dataRow.setField(16, entity1.getBusiness_manager());
                dataRow.setField(17, entity1.getBusiness_action());
                dataRow.setField(18, entity1.getBusiness_value());
                dataRow.setField(19, entity1.getBusiness_cost_FullTime());
                dataRow.setField(20, entity1.getBusiness_cost_Inhouse());
                dataRow.setField(21, entity1.getBusiness_cost_outsourced());
                dataRow.setField(22, entity1.getResource_planning_operations());
                dataRow.setField(23, entity1.getResource_planning_develop());
                dataRow.setField(24, entity1.getResource_planning_outsourcing());
                dataRow.setField(25, entity1.getProject_ework_operations());
                dataRow.setField(26, entity1.getProject_ework_develop());
                dataRow.setField(27, entity1.getProject_ework_outsourcing());
                dataRow.setField(28, entity1.getIT_CPU());
                dataRow.setField(29, entity1.getIT_memory());
                dataRow.setField(30, entity1.getIT_storage());
                dataRow.setField(31, entity1.getIT_DB());
                dataRow.setField(32, entity1.getIT_PAAS());
                dataRow.setField(33, entity1.getIT_pubilc());
                dataRow.setField(34, entity1.getLevel_group3());
                dataRow.setField(35, entity1.getResource_estimation());
                dataRow.setField(36, entity1.getResource_real_consumption());
                dataRow.setField(37, entity1.getWorkspace_name());
                dataRow.setAssigner(null);
//                        System.out.println(dataRow);
            }
        } else {
            System.exit(-1);        //为空则异常退出
        }



    }

     */


    //联合
    public static void main(String[] args) throws Exception{
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";//测试数据200条

        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";


        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        ObjectMapper objectMapper1 = new ObjectMapper();
        ObjectMapper objectMapper2 = new ObjectMapper();
        ObjectMapper objectMapper3 = new ObjectMapper();


        String result1 = GetHttpContent(url, auth);

        String result2 = GetHttpContent(url2, auth);//第二页的数据

        JsonNode jsonNode = objectMapper1.readTree(result1);
        JsonNode jsonNode22 = objectMapper2.readTree(result2);
        List<BusinessProflile> list1 = new ArrayList<>();
        ArrayList<String> arrayList = new ArrayList<>();

        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");
            for (JsonNode m : message) {
                String content = m.get("TObjectInstance").toString();
                arrayList.add(content);
            }
        }

        if (jsonNode22.has("data")) {
            JsonNode m2 = jsonNode22.get("data");
            for (JsonNode m : m2) {
                String content = m.get("TObjectInstance").toString();
                arrayList.add(content);
            }
        }

        list1 = JSONObject.parseArray(arrayList.toString(), BusinessProflile.class);

        if (CollectionUtils.isNotEmpty(list1)) {
            for (BusinessProflile entity1 : list1) {
                int tobject_instance_id = Integer.parseInt(entity1.getId());
                String ReUrl = String.format("http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?workspace_id=69993035&system_name=business_list&rel_system_name=project_list&tobject_instance_id=%d", tobject_instance_id);
                String reStr = GetHttpContent(ReUrl, auth);
                JsonNode jsonNode2 = objectMapper3.readTree(reStr);
                if (jsonNode.has("data")) {
                    JsonNode reProjectId = jsonNode2.get("data").findValue("id");//依赖的年度项目id
                    if (reProjectId != null) {

                        DataRow dataRow = new DataRow(2);
                        dataRow.setField(0, tobject_instance_id);
                        dataRow.setField(1, reProjectId.asInt());
                        dataRow.setAssigner(null);
                        System.out.println(dataRow);
                    }
                }
            }
        }
    }
}