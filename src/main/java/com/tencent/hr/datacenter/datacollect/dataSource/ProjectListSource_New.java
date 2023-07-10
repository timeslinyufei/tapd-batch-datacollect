package com.tencent.hr.datacenter.datacollect.dataSource;

import com.alibaba.fastjson.JSONObject;
import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class ProjectListSource_New implements SourceFunction<ProjectListBean> {
    @Override
    public void run(SourceContext<ProjectListBean> ctx) throws Exception {
        //年度项目数据
        //真正环境
//        String url = "http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200";
        //测试环境
        String url = Constant.PROJECTLISTSOURCE_NEW_URL;
        String auth = Constant.AUTH;

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
                ProjectListBean dataRow = new ProjectListBean();
                for (JsonNode m : message) {
                    String content = m.get("TObjectInstance").toString();
                    arrayList.add(content);
                }

                list1 = JSONObject.parseArray(arrayList.toString(), ProjectListBean.class);

                if (CollectionUtils.isNotEmpty(list1)) {
                    for (ProjectListBean entity1 : list1) {
                        dataRow.setId(entity1.getId());
                        dataRow.setWorkspace_id(entity1.getWorkspace_id());
                        dataRow.setTworkflow_is_done(entity1.getTworkflow_is_done());
                        dataRow.setTworkflow_id(entity1.getTworkflow_id());
                        dataRow.setName(entity1.getName());
                        dataRow.setDescription(entity1.getDescription());
                        dataRow.setCreated(entity1.getCreated());
                        dataRow.setModifier(entity1.getModifier());
                        dataRow.setModified(entity1.getModified());
                        dataRow.setTworkflow_step_name(entity1.getTworkflow_step_name());
                        dataRow.setTworkflow_statu_owner(entity1.getTworkflow_statu_owner());
                        dataRow.setTworkflow_entry_id(entity1.getTworkflow_entry_id());
                        dataRow.setProject_sort(entity1.getProject_sort());
                        dataRow.setBusiness_area(entity1.getBusiness_area());
                        dataRow.setBusiness_segment(entity1.getBusiness_segment());
                        dataRow.setProject_cycle_finish_time(entity1.getProject_cycle_finish_time());
                        dataRow.setProject_cycle_start_time(entity1.getProject_cycle_start_time());
                        dataRow.setKR(entity1.getKR());
                        dataRow.setJoin_OKR(entity1.getJoin_OKR());
                        dataRow.setNew_this_year(entity1.getNew_this_year());
                        dataRow.setProject_level(entity1.getProject_level());
                        dataRow.setStage_of_project(entity1.getStage_of_project());
                        dataRow.setProject_progre_notes(entity1.getProject_progre_notes());
                        dataRow.setPM(entity1.getPM());
                        dataRow.setSponsor(entity1.getSponsor());
                        dataRow.setExecution_PM((entity1.getExecution_PM() == null ? entity1.getExecution_PM() : entity1.getExecution_PM().replaceAll("(\r|\n)", "---")));
                        dataRow.setBackground((entity1.getBackground() == null ? entity1.getBackground() : entity1.getBackground().replaceAll("(\r|\n)", "---")));
                        dataRow.setBusiness_goal((entity1.getBusiness_goal() == null ? entity1.getBusiness_goal() : entity1.getBusiness_goal().replaceAll("(\r|\n)", "---")));
                        dataRow.setSolution((entity1.getSolution() == null ? entity1.getSolution() : entity1.getSolution().replaceAll("(\r|\n)", "---")));
                        dataRow.setMetrics((entity1.getMetrics() == null ? entity1.getMetrics() : entity1.getMetrics().replaceAll("(\r|\n)", "---")));
                        dataRow.setExpected_risk((entity1.getExpected_risk() == null ? entity1.getExpected_risk() : entity1.getExpected_risk().replaceAll("(\r|\n)", "---")));
                        dataRow.setExecution_PM((entity1.getExecution_PM()));
//                        dataRow.setBackground((entity1.getBackground()));
//                        dataRow.setBusiness_goal((entity1.getBusiness_goal()));
//                        dataRow.setSolution((entity1.getSolution()));
//                        dataRow.setMetrics((entity1.getMetrics()));
//                        dataRow.setExpected_risk((entity1.getExpected_risk()));

                        dataRow.setProject_member(entity1.getProject_member());
                        dataRow.setProject_stakeholder(entity1.getProject_stakeholder());
                        dataRow.setResource_planning_operations(entity1.getResource_planning_operations());
                        dataRow.setResource_planning_develop(entity1.getResource_planning_develop());
                        dataRow.setResource_planning_outsourcing(entity1.getResource_planning_outsourcing());
                        dataRow.setDocument_Space(entity1.getDocument_Space());
                        dataRow.setProject_progress_percentage(entity1.getProject_progress_percentage());
                        dataRow.setRdp_id(entity1.getRdp_id());
                        dataRow.setProjects((entity1.getProjects() == null ? entity1.getProjects() : entity1.getProjects().replaceAll("(\r|\n)", "---")));
//                        dataRow.setProjects((entity1.getProjects()));
                        dataRow.setProject_type(entity1.getProject_type());
                        dataRow.setProj_review_recommendations((entity1.getProj_review_recommendations() == null ? entity1.getProj_review_recommendations() : entity1.getProj_review_recommendations().replaceAll("(\r|\n)", "---")));
//                        dataRow.setProj_review_recommendations((entity1.getProj_review_recommendations()));
                        dataRow.setBusiness_area_new(entity1.getBusiness_area_new());
                        dataRow.setResource_estimation(entity1.getResource_estimation());
                        dataRow.setResource_real_consumption(entity1.getResource_real_consumption());
                        dataRow.setWorkspace_name(entity1.getWorkspace_name());
                        ctx.collect(dataRow);
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
