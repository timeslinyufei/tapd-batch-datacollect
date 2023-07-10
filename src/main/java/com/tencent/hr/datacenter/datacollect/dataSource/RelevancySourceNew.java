package com.tencent.hr.datacenter.datacollect.dataSource;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.datacollect.entity.RelevancyTableBean;
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
import java.util.Iterator;
import java.util.List;

public class RelevancySourceNew implements SourceFunction<RelevancyTableBean> {
    @Override
    public void run(SourceContext<RelevancyTableBean> ctx) throws Exception {
        //年度项目数据

        //TODO 测试
        //业务数据连接
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条，需要做一个append
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";

        //正式环境
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";//测试数据200条
//
//        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";

        //测试环境
        String url = Constant.BUSINESSSOURCE_NEW_URL1;//测试数据200条

        String url2 = Constant.BUSINESSSOURCE_NEW_URL2;

        String auth = Constant.AUTH;

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
        RelevancyTableBean relevancyTableBean = new RelevancyTableBean();

        if (CollectionUtils.isNotEmpty(list1)) {
            for (BusinessProflile entity1 : list1) {
                int tobject_instance_id = Integer.parseInt(entity1.getId());
                //正式环境
//                String ReUrl = String.format("http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?workspace_id=69993035&system_name=business_list&rel_system_name=project_list&tobject_instance_id=%d", tobject_instance_id);
                //测试环境
                String ReUrl = String.format(Constant.RELEVANCYSOURCE_NEW_URL2, tobject_instance_id);
                String reStr = GetHttpContent(ReUrl, auth);
                JsonNode jsonNode2 = objectMapper3.readTree(reStr);
                if (jsonNode.has("data")) {
//                    if (tobject_instance_id==10464){
//                        System.out.println("2================id======"+jsonNode2.get("data").findValue("id"));
                        Iterator<JsonNode> it = jsonNode2.get("data").elements();
                        while (it.hasNext()){
                            JsonNode itemNode=it.next();
                            JsonNode nameNode=itemNode.get("TObjectInstance").get("id");
                            if (nameNode != null) {
                                relevancyTableBean.setBusinessId(String.valueOf(tobject_instance_id));
                                relevancyTableBean.setProjectId(String.valueOf(nameNode.asInt()));

                                ctx.collect(relevancyTableBean);
                            }
                        }
//                    }

//                    JsonNode reProjectId = jsonNode2.get("data").findValue("id");//依赖的年度项目id
//                    if (reProjectId != null) {
//
//
//                        relevancyTableBean.setBusinessId(String.valueOf(tobject_instance_id));
//                        relevancyTableBean.setProjectId(String.valueOf(reProjectId.asInt()));
//
//                        ctx.collect(relevancyTableBean);
//                    }
                }
            }
        }
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

    @Override
    public void cancel() {

    }
}
