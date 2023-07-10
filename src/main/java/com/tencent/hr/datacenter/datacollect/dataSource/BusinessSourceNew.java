package com.tencent.hr.datacenter.datacollect.dataSource;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.flink.template.DataRow;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BusinessSourceNew implements SourceFunction<BusinessProflile> {

    public String GetHttpContent(String url, String auth) throws IOException {
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


    @Override
    public void run(SourceContext<BusinessProflile> ctx) throws IOException {
        //new version

        //真实环境
//        //业务档案数据
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
//        //业务数据有369条。接口只能一次访问200条数据，需要将数据结合
//        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
        //测试环境
        String url = Constant.BUSINESSSOURCE_NEW_URL1;
        //业务数据有369条。接口只能一次访问200条数据，需要将数据结合
        String url2 = Constant.BUSINESSSOURCE_NEW_URL2;
        String auth = Constant.AUTH;

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
            }
        }
        if (jsonNode2.has("data")) {
            JsonNode m2 = jsonNode2.get("data");
            for (JsonNode m : m2) {
                String content1 = m.get("TObjectInstance").toString();
                arrayList.add(content1);
            }
        }


        list1 = JSONObject.parseArray(arrayList.toString().replaceAll("3level_group", "level_group3"), BusinessProflile.class);

        BusinessProflile dataRow = new BusinessProflile();
        if (CollectionUtils.isNotEmpty(list1)) {
            for (BusinessProflile entity1 : list1) {
                dataRow.setId(entity1.getId());
                dataRow.setWorkspace_id(entity1.getWorkspace_id());
                dataRow.setTworkflow_is_done(entity1.getTworkflow_is_done());
                dataRow.setTworkflow_id(entity1.getTworkflow_id());
                dataRow.setName(entity1.getName() == null ? entity1.getName() : entity1.getName().replaceAll("(\r|\n)", " "));//
//                dataRow.setName(entity1.getName());
                dataRow.setDescription((entity1.getDescription() == null ? entity1.getDescription() : entity1.getDescription().replaceAll("(\r|\n)", "---")));
//                dataRow.setDescription(entity1.getDescription());
                dataRow.setCreated(entity1.getCreated());
                dataRow.setModifier(entity1.getModifier());
                dataRow.setModified(entity1.getModified());
                dataRow.setTworkflow_step_name(entity1.getTworkflow_step_name());
                dataRow.setTworkflow_statu_owner(entity1.getTworkflow_statu_owner());
                dataRow.setTworkflow_entry_id(entity1.getTworkflow_entry_id());
                dataRow.setStrategic_positioning(entity1.getStrategic_positioning());
                dataRow.setCore_functions(entity1.getCore_functions());
                dataRow.setSpecific_duties(entity1.getSpecific_duties());
                dataRow.setMethod_to_realize(entity1.getMethod_to_realize());
                dataRow.setBusiness_manager(entity1.getBusiness_manager());
                dataRow.setBusiness_action(entity1.getBusiness_action());
                dataRow.setBusiness_value(entity1.getBusiness_value());
                dataRow.setBusiness_cost_FullTime(entity1.getBusiness_cost_FullTime());
                dataRow.setBusiness_cost_Inhouse(entity1.getBusiness_cost_Inhouse());
                dataRow.setBusiness_cost_outsourced(entity1.getBusiness_cost_outsourced());
                dataRow.setResource_planning_operations(entity1.getResource_planning_operations());
                dataRow.setResource_planning_develop(entity1.getResource_planning_develop());
                dataRow.setResource_planning_outsourcing(entity1.getResource_planning_outsourcing());
                dataRow.setProject_ework_operations(entity1.getProject_ework_operations());
                dataRow.setProject_ework_develop(entity1.getProject_ework_develop());
                dataRow.setProject_ework_outsourcing(entity1.getProject_ework_outsourcing());
                dataRow.setIT_CPU(entity1.getIT_CPU());
                dataRow.setIT_memory(entity1.getIT_memory());
                dataRow.setIT_storage(entity1.getIT_storage());
                dataRow.setIT_DB(entity1.getIT_DB());
                dataRow.setIT_PAAS(entity1.getIT_PAAS());
                dataRow.setIT_pubilc(entity1.getIT_pubilc());
//                dataRow.setLevel_group3(unicdeToStringAndReplaceS(entity1.getLevel_group3()));
                dataRow.setLevel_group3(entity1.getLevel_group3());
                dataRow.setBusiness_project_outsourcing(entity1.getBusiness_project_outsourcing());
                dataRow.setConsulting_fees(entity1.getConsulting_fees());
                dataRow.setIndirect_cost(entity1.getIndirect_cost());
                dataRow.setIT_public(entity1.getIT_public());

                dataRow.setResource_estimation(entity1.getResource_estimation());
                dataRow.setResource_real_consumption(entity1.getResource_real_consumption());
                dataRow.setWorkspace_name(entity1.getWorkspace_name());
//                System.out.println(dataRow);
                ctx.collect(dataRow);

            }
        } else {
            System.exit(-1);        //为空则异常退出
        }
    }

    @Override
    public void cancel() {

    }

    public static String unicdeToStringAndReplaceS(String str) {
//        str=str.replaceAll("\\[","").replaceAll("\\]","");
        if (str == null) return null;
        if (str.equals("[]")) return str;
        Pattern pattern = Pattern.compile("(\\\\u(\\w{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
//            str = str.replace(matcher.group(1), ch + "");
            str = str.replace(matcher.group(1), String.valueOf(ch));
        }
        return str;
    }
}
