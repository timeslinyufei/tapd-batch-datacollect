package com.tencent.hr.datacenter.datacollect.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProjectListBean {
    private String id;
    private String workspace_id;
    private String tworkflow_is_done;
    private String tworkflow_id;
    private String name;
    private String description;
    private String creator;
    private String created;
    private String modifier;
    private String modified;
    private String tworkflow_step_name;
    private String tworkflow_statu_owner;
    private String tworkflow_entry_id;
    private String project_sort;
    private String business_area;
    private String business_segment;
    private String project_cycle_finish_time;
    private String project_cycle_start_time;
    private String KR;
    private String join_OKR;
    private String new_this_year;
    private String project_level;
    private String stage_of_project;
    private String project_progre_notes;
    private String PM;
    private String Sponsor;
    private String execution_PM;
    private String background;
    private String business_goal;
    private String solution;
    private String metrics;
    private String expected_risk;
    private String project_member;
    private String project_stakeholder;
    private String resource_planning_operations;
    private String resource_planning_develop;
    private String resource_planning_outsourcing;
    private String Document_Space;
    private String project_progress_percentage;
    private String rdp_id;
    private String projects;
    private String project_type;
    private String proj_review_recommendations;
    private String business_area_new;
    private String resource_estimation;
    private String resource_real_consumption;
    private String workspace_name;

}