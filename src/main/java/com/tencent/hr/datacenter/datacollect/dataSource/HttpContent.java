package com.tencent.hr.datacenter.datacollect.dataSource;

//import cn.hutool.core.util.StrUtil;
//import cn.hutool.http.ContentType;
//import cn.hutool.http.Header;


import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HttpContent {
    public static void main(String[] args) throws IOException {
        init();

        /*
                //测试环境  null测试
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=10461";
        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";
        String result1 = GetHttpContent(url, auth);

        ObjectMapper objectMapper1 = new ObjectMapper();
        JsonNode jsonNode = objectMapper1.readTree(result1);
        if (jsonNode.has("data")) {
            JsonNode message = jsonNode.get("data");
            System.out.println(message);
            for (JsonNode m : message) {
                JsonNode mm = m.get("TObjectInstance");
                JsonNode resourceEstimation = mm.get("resource_estimation");

                System.out.println(resourceEstimation);
            }

        }
         */


    }

    public static String init() throws IOException {
        //正式环境链接
//        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";


//测试链接
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=10457";
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=annual_projects_test&workspace_id=70025241&id=10051";
        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";
        String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));
        JSONObject data = new JSONObject();
//        Map data = new HashMap<String,String>();
        data.put("system_name", "business_profile_test");
        data.put("id", "10457");
        data.put("workspace_id", "70025241");
        data.put("tworkflow_is_done", "0");
        data.put("tworkflow_id", null);
        data.put("name", "0424~TEST 二级业务".replaceAll("~"," "));
        data.put("description", null);
        data.put("creator", "v_yuxiiyang");
        data.put("created", "2023-04-24 10:18:43");
        data.put("modifier", "cheshi");
        data.put("modified", "2023-04-24 10:50:11");
        data.put("tworkflow_step_name", null);
        data.put("tworkflow_statu_owner", null);
        data.put("tworkflow_entry_id", null);
        data.put("strategic_positioning", null);
        data.put("core_functions", null);
        data.put("specific_duties", "个体自驱性课题研究");
        data.put("method_to_realize", "系统");
        data.put("business_manager", "v_yuxiiyang;");
        data.put("business_action", "业务孵化");
        data.put("business_value", "高");
        data.put("business_cost_FullTime", "3");
        data.put("business_cost_Inhouse", "2");
        data.put("business_cost_outsourced", "3");
        data.put("resource_planning_operations", null);
        data.put("resource_planning_develop", null);
        data.put("resource_planning_outsourcing", null);
        data.put("project_ework_operations", null);
        data.put("project_ework_develop", null);
        data.put("project_ework_outsourcing", null);
        data.put("IT_CPU", null);
        data.put("IT_memory", null);
        data.put("IT_storage", null);
        data.put("IT_DB", null);
        data.put("3level_group", "招活研发交付组");
        data.put("business_project_outsourcing", "1");
        data.put("consulting_fees", "40");
        data.put("indirect_cost", null);
        data.put("IT_public", null);
        data.put("resource_estimation", "78");
        data.put("resource_real_consumption", "99");
        data.put("workspace_name", "SDC业务档案(测试)");//





//        hutoolTest(url, encoding, data);

        PostHttpContent(url, encoding, data);
        return "sucess";
    }


    //    public static void hutoolTest(String url, String encoding, JSONObject data) throws IOException {
//        HttpRequest.post(url)
//                .header("Authorization", "Basic " + encoding)
//                .body(String.valueOf(data))
//                .execute();
//    }
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
//            connection.setRequestProperty("Accept-Charset","UTF-8");
            connection.setRequestProperty("Connection", "Keep-Alive");// 维持长连接
//            connection.setRequestProperty("Transfer-Encoding","UTF-8");
            connection.setRequestProperty("Charset", "UTF-8");
            connection.setRequestProperty("accept", "*/*");
            String authHeaderValue = "Basic " + new String(encoding);
            connection.setRequestProperty("Authorization", authHeaderValue);
            connection.setDoOutput(true);
            connection.setDoInput(true);


            System.out.println(data.toJSONString());
//            OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(),"UTF-8");

            //建立输入流，向指向的URL传入参数
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());


//            out.write(JSONObject.toJSONString(data));
            out.write(data.toJSONString().getBytes(StandardCharsets.UTF_8));
//            out.write(sbParams.toString());
            out.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));//,"UTF-8"
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                String result = JSONObject.parse(inputLine).toString();
                System.out.println(inputLine);
                System.out.println(result);
                response.append(result);
            }


            in.close();
//            System.out.println(response.toString());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

