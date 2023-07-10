package com.tencent.hr.datacenter.datacollect.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlElement;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BusinessProflile {
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
    private String strategic_positioning;
    private String core_functions;
    private String specific_duties;
    private String method_to_realize;
    private String business_manager;
    private String business_action;
    private String business_value;
    private String business_cost_FullTime;
    private String business_cost_Inhouse;
    private String business_cost_outsourced;
    private String resource_planning_operations;
    private String resource_planning_develop;
    private String resource_planning_outsourcing;
    private String business_ework_fulltime;
    private String business_ework_inhouse;
    private String business_ework_outsourced;
    private String project_ework_operations;
    private String project_ework_develop;
    private String project_ework_outsourcing;
    private String IT_CPU;
    private String IT_memory;
    private String IT_storage;
    private String IT_DB;
    private String IT_PAAS;
    private String IT_pubilc;
//    @XmlElement(name = "3level_group") // 使用XmlElement注解
    private String level_group3;

    //新增
    private String business_project_outsourcing;
    private String consulting_fees;
    private String indirect_cost;
    private String IT_public;
    //新增
    private String resource_estimation;
    private String resource_real_consumption;
    private String workspace_name;

}