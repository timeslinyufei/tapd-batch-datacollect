package com.tencent.hr.datacenter.datacollect;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.entity.BusinessProflile;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
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


public class ProjectListSource extends AbstractCustomSource {


    @Override
    public void init() throws Exception {

    }

    @Override
    public void collectData(SourceContext<DataRow> sourceContext) throws Exception {
        //年度项目数据
        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200";
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
                        dataRow.setField(25, (entity1.getExecution_PM() == null ? entity1.getExecution_PM() : entity1.getExecution_PM().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(26, (entity1.getBackground() == null ? entity1.getBackground() : entity1.getBackground().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(27, (entity1.getBusiness_goal() == null ? entity1.getBusiness_goal() : entity1.getBusiness_goal().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(28, (entity1.getSolution() == null ? entity1.getSolution() : entity1.getSolution().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(29, (entity1.getMetrics() == null ? entity1.getMetrics() : entity1.getMetrics().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(30, (entity1.getExpected_risk() == null ? entity1.getExpected_risk() : entity1.getExpected_risk().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(31, entity1.getProject_member());
                        dataRow.setField(32, entity1.getProject_stakeholder());
                        dataRow.setField(33, entity1.getResource_planning_operations());
                        dataRow.setField(34, entity1.getResource_planning_develop());
                        dataRow.setField(35, entity1.getResource_planning_outsourcing());
                        dataRow.setField(36, entity1.getDocument_Space());
                        dataRow.setField(37, entity1.getProject_progress_percentage());
                        dataRow.setField(38, entity1.getRdp_id());
                        dataRow.setField(39, (entity1.getProjects() == null ? entity1.getProjects() : entity1.getProjects().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(40, entity1.getProject_type());
                        dataRow.setField(41, (entity1.getProj_review_recommendations() == null ? entity1.getProj_review_recommendations() : entity1.getProj_review_recommendations().replaceAll("(\r|\n)+", "---")));
                        dataRow.setField(42, entity1.getBusiness_area_new());
                        dataRow.setField(43, entity1.getResource_estimation());
                        dataRow.setField(44, entity1.getResource_real_consumption());
                        dataRow.setField(45, entity1.getWorkspace_name());
                        dataRow.setAssigner(null);
                        sourceContext.collect(dataRow);
                    }
                } else {
                    System.exit(-1);        //为空则异常退出
                }


            }
            br.close();
            input.close();
        }


    }


    @Override
    public void cancel() {

    }

}
