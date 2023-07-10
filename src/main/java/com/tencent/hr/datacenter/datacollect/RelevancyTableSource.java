package com.tencent.hr.datacenter.datacollect;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
import com.tencent.hr.datacenter.datacollect.entity.RelevancyTableBean;
import com.tencent.hr.datacenter.flink.template.AbstractCustomSource;
import com.tencent.hr.datacenter.flink.template.DataRow;
import com.tencent.hr.datacenter.flink.template.utils.DateUtil;
import com.tencent.hr.datacenter.flink.template.utils.HttpUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


public class RelevancyTableSource extends AbstractCustomSource {


    @Override
    public void init() throws Exception {

    }

    @Override
    public void collectData(SourceContext<DataRow> sourceContext) throws Exception {
        //年度项目数据

        //TODO 测试
        //业务数据连接
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条，需要做一个append
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
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
                            sourceContext.collect(dataRow);
                        }
                    }
                }
            }
    }

    @Override
    public void cancel() {

    }

    private String GetHttpContent(String url, String auth) throws IOException {
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

}
