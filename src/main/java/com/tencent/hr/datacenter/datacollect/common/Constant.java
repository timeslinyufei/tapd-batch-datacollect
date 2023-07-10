package com.tencent.hr.datacenter.datacollect.common;


public class Constant {



    //数据来源

    //  年度测试链接url
//    public static final String PROJECTLISTSOURCE_NEW_URL = "http://apiv2.tapd.oa.com/tobjects?system_name=annual_projects_test&workspace_id=70025241&limit=200";//http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200
    public static final String PROJECTLISTSOURCE_NEW_URL = "http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&limit=200";
    //业务数据测试链接url1
//    public static final String BUSINESSSOURCE_NEW_URL1 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&limit=200&page=1";//"http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1"
    public static final String BUSINESSSOURCE_NEW_URL1 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=1";
    //业务数据测试链接url2
//    public static final String BUSINESSSOURCE_NEW_URL2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&limit=200&page=2";// "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2"
    public static final String BUSINESSSOURCE_NEW_URL2 = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&limit=200&page=2";
    //    public static final String RELEVANCYSOURCE_NEW_URL2 = "http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?system_name=business_profile_test&workspace_id=70025241&rel_system_name=annual_projects_test&tobject_instance_id=%d";//"http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?workspace_id=69993035&system_name=business_list&rel_system_name=project_list&tobject_instance_id=%d
    public static final String RELEVANCYSOURCE_NEW_URL2 = "http://apiv2.tapd.oa.com/tobjects/get_relative_tobject_instances?workspace_id=69993035&system_name=business_list&rel_system_name=project_list&tobject_instance_id=%d";//"



    //配置资源

    //测试环境的业务url
//    public static final String BUSINESSURL = "http://apiv2.tapd.oa.com/tobjects?system_name=business_profile_test&workspace_id=70025241&id=%d";//"http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&id=%d"
    public static final String BUSINESSURL = "http://apiv2.tapd.oa.com/tobjects?system_name=business_list&workspace_id=69993035&id=%d";
    public static final String AUTH = "hrdw" + ":" + "30965827-6FF9-AC1B-8D50-ABDA7554F88E";
    //    public static final String SYSTEM_NAEM_BUSINESS = "business_profile_test";//business_list
    public static final String SYSTEM_NAEM_BUSINESS = "business_list";

//    public static final String RE_SQL = "select  " +
//            "    bus2.id,  " +
//            "    bus2.workspace_id,  " +
//            "    bus2.tworkflow_is_done,  " +
//            "    bus2.tworkflow_id,  " +
//            "    bus2.name,  " +
//            "    bus2.description,  " +
//            "    bus2.creator,  " +
//            "    bus2.created,  " +
//            "    bus2.modifier,  " +
//            "    bus2.modified,  " +
//            "    bus2.tworkflow_step_name,  " +
//            "    bus2.tworkflow_statu_owner,  " +
//            "    bus2.tworkflow_entry_id,  " +
//            "    bus2.strategic_positioning,  " +
//            "    bus2.core_functions,  " +
//            "    bus2.specific_duties,  " +
//            "    bus2.method_to_realize,  " +
//            "    bus2.business_manager,  " +
//            "    bus2.business_action,  " +
//            "    bus2.business_value,  " +
//            "    bus2.business_cost_FullTime,  " +
//            "    bus2.business_cost_Inhouse,  " +
//            "    bus2.business_cost_outsourced,  " +
//            "    bus2.resource_planning_operations,  " +
//            "    bus2.resource_planning_develop,  " +
//            "    bus2.resource_planning_outsourcing,  " +
//            "    bus2.business_ework_fulltime,  " +
//            "    bus2.business_ework_inhouse,  " +
//            "    bus2.business_ework_outsourced,  " +
//            "    bus2.project_ework_operations,  " +
//            "    bus2.project_ework_develop,  " +
//            "    bus2.project_ework_outsourcing,  " +
//            "    bus2.IT_CPU,  " +
//            "    bus2.IT_memory,  " +
//            "    bus2.IT_storage,  " +
//            "    bus2.IT_DB,  " +
//            "    bus2.IT_PAAS,  " +
//            "    bus2.IT_pubilc,  " +
//            "    bus2.level_group3,  " +
//            "    bus2.business_project_outsourcing,  " +
//            "    bus2.consulting_fees,  " +
//            "    bus2.indirect_cost,  " +
//            "    bus2.IT_public,  " +
//            "    t1.resource_estimation,  " +
//            "    bus2.resource_real_consumption,  " +
//            "    bus2.workspace_name  " +
//            "from  " +
//            "    (  " +
//            "    select  " +
//            "    id,  " +
//            "    resource_estimation  " +
//            "    from  " +
//            "    (  " +
//            "        select  " +
//            "            t2.id,  " +
//            "            t2.resource_estimation,  " +
//            "             row_number() over(partition by id order by resource_estimation desc)  rk  " +
//            "        from(  " +
//            "         select     " +
//            "             bus.id,     " +
//            "             cast(     " +
//            "                 sum(cast(pj.resource_estimation as INT)) as STRING     " +
//            "             ) AS resource_estimation   " +
//            "         from     " +
//            "             business as bus     " +
//            "             join relevancy as re on bus.id = re.BusinessId     " +
//            "             join projectlist as pj on re.ProjectId = pj.id     " +
//            "         group by     " +
//            "             bus.id    " +
//            "        )t2  " +
//            "    )t3  " +
//            "    where rk=1" +
//            "    ) t1  " +
//            "   inner join business bus2 on bus2.id = t1.id" ;

    public static final String RE_SQL = "     select bus.id,    " +
            "             bus.workspace_id   ,   " +
            "             bus.tworkflow_is_done   ,   " +
            "             bus.tworkflow_id   ,   " +
            "             bus.name   ,   " +
            "             bus.description   ,   " +
            "             bus.creator   ,   " +
            "             bus.created   ,   " +
            "             bus.modifier   ,   " +
            "             bus.modified   ,   " +
            "             bus.tworkflow_step_name   ,   " +
            "             bus.tworkflow_statu_owner   ,   " +
            "             bus.tworkflow_entry_id   ,   " +
            "             bus.strategic_positioning   ,   " +
            "             bus.core_functions   ,   " +
            "             bus.specific_duties   ,   " +
            "             bus.method_to_realize   ,   " +
            "             bus.business_manager   ,   " +
            "             bus.business_action   ,   " +
            "             bus.business_value   ,   " +
            "             bus.business_cost_FullTime   ,   " +
            "             bus.business_cost_Inhouse   ,   " +
            "             bus.business_cost_outsourced   ,   " +
            "             bus.resource_planning_operations   ,   " +
            "             bus.resource_planning_develop   ,   " +
            "             bus.resource_planning_outsourcing   ,   " +
            "             bus.business_ework_fulltime   ,   " +
            "             bus.business_ework_inhouse   ,   " +
            "             bus.business_ework_outsourced   ,   " +
            "             bus.project_ework_operations   ,   " +
            "             bus.project_ework_develop   ,   " +
            "             bus.project_ework_outsourcing   ,   " +
            "             bus.IT_CPU   ,   " +
            "             bus.IT_memory   ,   " +
            "             bus.IT_storage   ,   " +
            "             bus.IT_DB   ,   " +
            "             bus.IT_PAAS   ,   " +
            "             bus.IT_pubilc ,   " +
            "             bus.level_group3   ,   " +
            "             bus.business_project_outsourcing ,   " +
            "             bus.consulting_fees ,   " +
            "             bus.indirect_cost ,   " +
            "             bus.IT_public ,   " +
//                "             cast(cast(sum(cast(pj.resource_estimation as decimal(10,2))) as decimal)as STRING) AS resource_estimation ,   " +
            "             cast(sum(cast(pj.resource_estimation as decimal(10,2)))as STRING) AS resource_estimation ,   " +
            "             bus.resource_real_consumption   ,   " +
            "             bus.workspace_name      " +
            "             from business as bus  join relevancy as re on bus.id = re.BusinessId    " +
            "             join projectlist as pj on re.ProjectId = pj.id    " +
            "             group by     " +
            "             bus.id,    " +
            "             bus.workspace_id   ,   " +
            "             bus.tworkflow_is_done   ,   " +
            "             bus.tworkflow_id   ,   " +
            "             bus.name   ,   " +
            "             bus.description   ,   " +
            "             bus.creator   ,   " +
            "             bus.created   ,   " +
            "             bus.modifier   ,   " +
            "             bus.modified   ,   " +
            "             bus.tworkflow_step_name   ,   " +
            "             bus.tworkflow_statu_owner   ,   " +
            "             bus.tworkflow_entry_id   ,   " +
            "             bus.strategic_positioning   ,   " +
            "             bus.core_functions   ,   " +
            "             bus.specific_duties   ,   " +
            "             bus.method_to_realize   ,   " +
            "             bus.business_manager   ,   " +
            "             bus.business_action   ,   " +
            "             bus.business_value   ,   " +
            "             bus.business_cost_FullTime   ,   " +
            "             bus.business_cost_Inhouse   ,   " +
            "             bus.business_cost_outsourced   ,   " +
            "             bus.resource_planning_operations   ,   " +
            "             bus.resource_planning_develop   ,   " +
            "             bus.resource_planning_outsourcing   ,   " +
            "             bus.business_ework_fulltime   ,   " +
            "             bus.business_ework_inhouse   ,   " +
            "             bus.business_ework_outsourced   ,   " +
            "             bus.project_ework_operations   ,   " +
            "             bus.project_ework_develop   ,   " +
            "             bus.project_ework_outsourcing   ,   " +
            "             bus.IT_CPU   ,   " +
            "             bus.IT_memory   ,   " +
            "             bus.IT_storage   ,   " +
            "             bus.IT_DB   ,   " +
            "             bus.IT_PAAS   ,   " +
            "             bus.IT_pubilc   ,   " +
            "             bus.level_group3   ,   " +
            "             bus.business_project_outsourcing ,   " +
            "             bus.consulting_fees ,   " +
            "             bus.indirect_cost ,   " +
            "             bus.IT_public ,   " +
            "             bus.resource_real_consumption   ,   " +
            "             bus.workspace_name  " ;


    //人力资源

    //    public static final String PROFILE_URL ="http://apiv2.tapd.oa.com/tobjects?system_name=annual_projects_test&workspace_id=70025241&id=%d";//"http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&id=%d"
    public static final String PROFILE_URL ="http://apiv2.tapd.oa.com/tobjects?system_name=project_list&workspace_id=69993035&id=%d";//
    public static final String PG_DRIVER = "org.postgresql.Driver";
    public static final String PG_URL = "jdbc:postgresql://9.135.70.241:5432/postgres";
    public static final String PG_USER = "dpc";
    public static final String PG_PASSWORD = "dpc@2022";
    //    public static final String SYSTEM_NAME_PROFILE = "annual_projects_test";//project_list
    public static final String SYSTEM_NAME_PROFILE = "project_list";//

    //    e工时业务sql
    //业务没有sum年度的   e工时sql
    public static final String FTEBusSql1 =   "select bus.id, " +
            "bus.workspace_id   ," +
            "bus.tworkflow_is_done   ," +
            "bus.tworkflow_id   ," +
            "bus.name   ," +
            "bus.description   ," +
            "bus.creator   ," +
            "bus.created   ," +
            "bus.modifier   ," +
            "bus.modified   ," +
            "bus.tworkflow_step_name   ," +
            "bus.tworkflow_statu_owner   ," +
            "bus.tworkflow_entry_id   ," +
            "bus.strategic_positioning   ," +
            "bus.core_functions   ," +
            "bus.specific_duties   ," +
            "bus.method_to_realize   ," +
            "bus.business_manager   ," +
            "bus.business_action   ," +
            "bus.business_value   ," +
            "bus.business_cost_FullTime   ," +
            "bus.business_cost_Inhouse   ," +
            "bus.business_cost_outsourced   ," +
            "bus.resource_planning_operations   ," +
            "bus.resource_planning_develop   ," +
            "bus.resource_planning_outsourcing   ," +
            "bus.business_ework_fulltime   ," +
            "bus.business_ework_inhouse   ," +
            "bus.business_ework_outsourced   ," +
            "bus.project_ework_operations   ," +
            "bus.project_ework_develop   ," +
            "bus.project_ework_outsourcing   ," +
            "bus.IT_CPU   ," +
            "bus.IT_memory   ," +
            "bus.IT_storage   ," +
            "bus.IT_DB   ," +
            "bus.IT_PAAS   ," +
            "bus.IT_pubilc   ," +
            "bus.level_group3   ," +
            "bus.business_project_outsourcing ," +
            "bus.consulting_fees ," +
            "bus.indirect_cost ," +
            "bus.IT_public ," +
            "bus.resource_estimation ," +
            "e.FTE_number as resource_real_consumption   ," +
            "bus.workspace_name   " +
            "from business as bus join relevancy as re on bus.id = re.BusinessId " +
            "join projectlist as pj on re.ProjectId = pj.id " +
            "join Etc as e on e.rioid = pj.id ";


    public static final String FTEBusSql =   " select bus.id,  " +
            "        bus.workspace_id   , " +
            "        bus.tworkflow_is_done   , " +
            "        bus.tworkflow_id   , " +
            "        bus.name   , " +
            "        bus.description   , " +
            "        bus.creator   , " +
            "        bus.created   , " +
            "        bus.modifier   , " +
            "        bus.modified   , " +
            "        bus.tworkflow_step_name   , " +
            "        bus.tworkflow_statu_owner   , " +
            "        bus.tworkflow_entry_id   , " +
            "        bus.strategic_positioning   , " +
            "        bus.core_functions   , " +
            "        bus.specific_duties   , " +
            "        bus.method_to_realize   , " +
            "        bus.business_manager   , " +
            "        bus.business_action   , " +
            "        bus.business_value   , " +
            "        bus.business_cost_FullTime   , " +
            "        bus.business_cost_Inhouse   , " +
            "        bus.business_cost_outsourced   , " +
            "        bus.resource_planning_operations   , " +
            "        bus.resource_planning_develop   , " +
            "        bus.resource_planning_outsourcing   , " +
            "        bus.business_ework_fulltime   , " +
            "        bus.business_ework_inhouse   , " +
            "        bus.business_ework_outsourced   , " +
            "        bus.project_ework_operations   , " +
            "        bus.project_ework_develop   , " +
            "        bus.project_ework_outsourcing   , " +
            "        bus.IT_CPU   , " +
            "        bus.IT_memory   , " +
            "        bus.IT_storage   , " +
            "        bus.IT_DB   , " +
            "        bus.IT_PAAS   , " +
            "        bus.IT_pubilc   , " +
            "        bus.level_group3   , " +
            "        bus.business_project_outsourcing , " +
            "        bus.consulting_fees , " +
            "        bus.indirect_cost , " +
            "        bus.IT_public , " +
            "        bus.resource_estimation , " +
//            "        cast(cast(sum(cast(e.FTE_number as decimal(10,2))) as decimal) as STRING) AS resource_real_consumption , " +
            "        cast(sum(cast(e.FTE_number as decimal(10,2))) as STRING) AS resource_real_consumption , " +
            "        bus.workspace_name    " +
            "        from business as bus join relevancy as re on bus.id = re.BusinessId  " +
            "        join projectlist as pj on re.ProjectId = pj.id  " +
            "        join Etc as e on e.rioid = pj.id  " +
            "        group by      " +
            "         bus.id,     " +
            "         bus.workspace_id   ,    " +
            "         bus.tworkflow_is_done   ,    " +
            "         bus.tworkflow_id   ,    " +
            "         bus.name   ,    " +
            "         bus.description   ,    " +
            "         bus.creator   ,    " +
            "         bus.created   ,    " +
            "         bus.modifier   ,    " +
            "         bus.modified   ,    " +
            "         bus.tworkflow_step_name   ,    " +
            "         bus.tworkflow_statu_owner   ,    " +
            "         bus.tworkflow_entry_id   ,    " +
            "         bus.strategic_positioning   ,    " +
            "         bus.core_functions   ,    " +
            "         bus.specific_duties   ,    " +
            "         bus.method_to_realize   ,    " +
            "         bus.business_manager   ,    " +
            "         bus.business_action   ,    " +
            "         bus.business_value   ,    " +
            "         bus.business_cost_FullTime   ,    " +
            "         bus.business_cost_Inhouse   ,    " +
            "         bus.business_cost_outsourced   ,    " +
            "         bus.resource_planning_operations   ,    " +
            "         bus.resource_planning_develop   ,    " +
            "         bus.resource_planning_outsourcing   ,    " +
            "         bus.business_ework_fulltime   ,    " +
            "         bus.business_ework_inhouse   ,    " +
            "         bus.business_ework_outsourced   ,    " +
            "         bus.project_ework_operations   ,    " +
            "         bus.project_ework_develop   ,    " +
            "         bus.project_ework_outsourcing   ,    " +
            "         bus.IT_CPU   ,    " +
            "         bus.IT_memory   ,    " +
            "         bus.IT_storage   ,    " +
            "         bus.IT_DB   ,    " +
            "         bus.IT_PAAS   ,    " +
            "         bus.IT_pubilc   ,    " +
            "         bus.level_group3   ,    " +
            "         bus.business_project_outsourcing ,    " +
            "         bus.consulting_fees ,    " +
            "         bus.indirect_cost ,    " +
            "         bus.IT_public ,    " +
            "         bus.resource_estimation ,   " +
            "         bus.workspace_name   ";
    public static final String FTESql = "    with products_new as(  " +
            "select  " +
            " id,  " +
            "    name,  " +
            " reference ->> 'rio' as Rioid,  " +
            " unnest(members) Useid  " +
            "from  " +
            " products p  " +
            "where  " +
            " reference -> 'rio' is not null  " +
            " and reference ->> 'rio' not like 'P%'  " +
            " and reference ->> 'rio' not like 'R%'),  " +
            "user_new as (  " +
            "select  " +
            " id,  " +
            " name,  " +
            " split_part(tags_new, ':', 1) as \"number\",  " +
            "    split_part(tags_new, ':', 2) as \"encode\"  " +
            "from  " +
            " (  " +
            " select  " +
            "  id,  " +
            "  name,  " +
            "     unnest(string_to_array(btrim(tags::text, '{}'), ',')) tags_new  " +
            " from  " +
            "  users u   " +
            ") users_text),  " +
            "tags_new as (  " +
            "select  " +
            " id,  " +
            " name,  " +
            " split_part(tags_new, ',', 1) as \"encode\",  " +
            "    split_part(tags_new, ',', 2) as \"type\"  " +
            "from  " +
            " (  " +
            " select  " +
            "  id,  " +
            "  name,  " +
            "  btrim(jsonb_each_text(tags)::text, '()') as tags_new  " +
            " from  " +
            "  tags  " +
            ") tags_text   " +
            "),  " +
            "days as (  " +
            "select  " +
            " date(t) as dday  " +
            "from  " +
            " generate_series('2000-01-01'::date, '2099-12-31', '1 days') as t  " +
            "),  " +
            "workloads_new as (  " +
            "select id, " +
            "    user_id, " +
            "       btrim( ltrim( split_part( loads_new,':',1),' '),'\"\"') as \"proid\", " +
            "    split_part( loads_new,':',2) as \"hours\", " +
            "    start_time, " +
            "    end_time " +
            "from ( " +
            " select id, " +
            "     user_id, " +
            "     unnest(string_to_array(btrim(loads::text,'{}'),',')) loads_new, " +
            "     start_time, " +
            "     end_time " +
            " from (  select id, " +
            "       user_id, " +
            "       loads, " +
            "       split_part( btrim(period::text,'[)'),',',1) as \"start_time\", " +
            "       split_part( btrim(period::text,'[)'),',',2) as \"end_time\" " +
            "   from workloads w " +
            "         ) t1  ) t2  " +
            ")  " +
            "select rioid, " +
            "    round(sum(man_mon*typeid)::decimal,2) as \"FTE_number\" " +
            " from ( " +
            "  select rioid,user_id,mon,ddays, " +
            "         sum(workhour)/8/ddays as \"man_mon\", " +
            "      case when type = '外包人力' then 0.3333 " +
            "           when type = '子公司' then 0.6666 " +
            "           else 1 end as \"typeid\" " +
            "  from ( " +
            "   select p.rioid, " +
            "       w.user_id, " +
            "       t.type, " +
//            "          round(w.hours::decimal/(count(d.dday)over(partition by w.id) )::decimal,2) as \"workhour\", " +
            "          round(w.hours::decimal/(count(d.dday)over(partition by w.id,p.rioid) )::decimal,2) as \"workhour\", " +
            "          d.dday, " +
            "          to_char(d.dday,'MM') as \"mon\", " +
            "          wd.ddays  " +
            "   from workloads_new w join (select id,rioid from products_new group by id,rioid) p on w.proid=p.id::text " +
            "            join days d on w.start_time <= d.dday::text and w.end_time > d.dday::text " +
            "            join user_new u on w.user_id = u.id " +
            "            join tags_new t on u.encode = t.encode and t.id = 2 " +
            "            join workday wd on to_char(d.dday,'YYYY-MM') = wd.dday " +
            "            join legalworkday l on d.dday::text = l.dday and l.wday = 1 " +
            "   where to_char(d.dday,'YYYY') = to_char(now(),'YYYY') " +
            "  ) t1 " +
            "  group by rioid,user_id,mon,ddays,type " +
            ") t2 " +
            "group by rioid";

}
