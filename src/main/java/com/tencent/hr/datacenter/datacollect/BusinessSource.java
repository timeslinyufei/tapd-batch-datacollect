package com.tencent.hr.datacenter.datacollect;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.flink.template.AbstractCustomSource;
import com.tencent.hr.datacenter.flink.template.DataRow;
import com.tencent.hr.datacenter.flink.template.utils.HttpUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


public class BusinessSource extends AbstractCustomSource {
    //    private static final Logger LOG = LoggerFactory.getLogger(DJGStaffSource.class);
    public static final int REQ_RETRY_COUNT = 3;
    private String api = "/api/DsForTsg/1.0/getHrCompanyStaff";
    private String url;
    private String appid;
    private String appsecret;
    private HttpUtil httpUtil;
    private String nonce;

    @Override
    public void init() throws Exception {
//        this.url = super.profileParams.getRequired("config.url");
//        this.appid = super.profileParams.getRequired("config.appid");
//        this.appsecret = super.profileParams.getRequired("config.appsecret");
//        this.nonce= UUID.randomUUID().toString().toUpperCase();
//        this.httpUtil = new HttpUtil();
//        LOG.info("url="+this.url);
//        LOG.info("appid="+appid);
    }

    @Override
    public void collectData(SourceContext<DataRow> sourceContext) throws Exception {
        //1、组装url及设置请求Header
//        String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
//        String params = String.format("appid=%s&nonce=%s&timestamp=%s", appid, nonce, timestamp);
//        String strSignatureTemp = String.format("%s&key=%s", params, appsecret);
//        String signature = Utils.encrypt(strSignatureTemp, appsecret, "HmacSHA256").toUpperCase();
//        String requestUrl = String.format("%s%s?%s&signature=%s",
//                this.url, this.api, params, signature);
//        LOG.info("url:" + requestUrl);
//        String result = httpUtil.getAndRetry(requestUrl, REQ_RETRY_COUNT);
//        List<HrCompanyStaffEntity> list = new ArrayList<>();
//        list = JSONObject.parseArray(result, HrCompanyStaffEntity.class);
//        String currentTime = DateUtil.dateTimeNow("yyyy-MM-dd HH:mm:ss");
//        if (CollectionUtils.isNotEmpty(list)) {
//            for (HrCompanyStaffEntity entity : list) {
//                DataRow dataRow = new DataRow(8);
//                dataRow.setField(0, currentTime);
//                dataRow.setField(1, entity.getCompanyId());
//                dataRow.setField(2, entity.getCompanyName());
//                dataRow.setField(3, Utils.checkNull(entity.getPersonType()));
//                dataRow.setField(4, Utils.checkNull(entity.getName()));
//                dataRow.setField(5, Utils.checkNull(entity.getEnterpriseWechat()));
//                dataRow.setField(6, Utils.checkNull(entity.getAppointDate()).replaceAll("/", "-"));
//                dataRow.setField(7, Utils.checkNull(entity.getExpiryDate()).replaceAll("/", "-"));
//                dataRow.setAssigner(null);
//                sourceContext.collect(dataRow);
//            }
//        } else {
//            System.exit(-1);        //为空则异常退出
//        }


        /*

        //业务档案数据
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
        //业务数据有369条。接口只能一次访问200条数据，需要将数据结合
        String url2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
        String auth = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";

        ObjectMapper objectMapper = new ObjectMapper();


        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);

        String encoding = DatatypeConverter.printBase64Binary(auth.getBytes("UTF-8"));

        httpGet.setHeader("Authorization", "Basic " + encoding);

        CloseableHttpResponse response = httpClient.execute(httpGet);
        int responseCode = response.getStatusLine().getStatusCode();


        String result2 = GetHttpContent(url2, auth);//第二页的数据


        if (responseCode == 200) {
            HttpEntity entity = response.getEntity();
            entity.getContent();

            InputStream input = entity.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            String str1 = br.readLine();
            String result1 = new String(str1.getBytes(StandardCharsets.US_ASCII), StandardCharsets.UTF_8);
            JsonNode jsonNode = objectMapper.readTree(result1);
            JsonNode jsonNode2 = objectMapper.readTree(result2);
            if (jsonNode.has("data")) {
                JsonNode message = jsonNode.get("data");

                List<BusinessProflile> list1 = new ArrayList<>();
                ArrayList<String> arrayList = new ArrayList<>();

                for (JsonNode m : message) {
                    String content = m.get("TObjectInstance").toString();
                    arrayList.add(content);
                }
                if (jsonNode2.has("data")) {
                    JsonNode m2 = jsonNode.get("data");
                    for (JsonNode m : m2) {
                        String content = m.get("TObjectInstance").toString();
                        arrayList.add(content);
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
                        dataRow.setField(4, entity1.getName()==null?entity1.getName():entity1.getName().replaceAll("(\r|\n)+", ""));
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
                        sourceContext.collect(dataRow);
                    }
                } else {
                    System.exit(-1);        //为空则异常退出
                }
//                }
            }
            br.close();
            input.close();
        }


         */


        //new version
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
                sourceContext.collect(dataRow);
            }
        } else {
            System.exit(-1);        //为空则异常退出
        }

    }


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

    @Override
    public void cancel() {

    }

}
