package com.example.flinkcdcdm.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Canal 风格的 JSON 消息模型（flat message 格式）.
 *
 * <p>字段说明：
 * <ul>
 *   <li>database - 数据库/Schema 名</li>
 *   <li>table    - 表名</li>
 *   <li>type     - 操作类型：INSERT / UPDATE / DELETE</li>
 *   <li>es       - 事件时间戳（event source timestamp，毫秒）</li>
 *   <li>ts       - 消息投递时间戳（毫秒）</li>
 *   <li>data     - 变更后的行数据（单行也封装为数组）</li>
 *   <li>old      - UPDATE 前的旧值（仅包含变更字段），非 UPDATE 为 null</li>
 *   <li>pkNames  - 主键列名列表</li>
 *   <li>isDdl    - 是否为 DDL 事件（DM CDC 暂不支持，固定 false）</li>
 *   <li>sql      - DDL 语句（非 DDL 时为空字符串）</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CanalMessage {

    private String database;
    private String table;
    private String type;
    /** 事件源时间戳（ms） */
    private Long es;
    /** 消息投递时间戳（ms） */
    private Long ts;
    private List<Map<String, Object>> data;
    /** UPDATE 时的旧值（仅变更字段），其他操作为 null */
    private List<Map<String, Object>> old;
    private List<String> pkNames;

    @JsonProperty("isDdl")
    private boolean isDdl = false;
    private String sql = "";

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Long getEs() { return es; }
    public void setEs(Long es) { this.es = es; }
    public Long getTs() { return ts; }
    public void setTs(Long ts) { this.ts = ts; }
    public List<Map<String, Object>> getData() { return data; }
    public void setData(List<Map<String, Object>> data) { this.data = data; }
    public List<Map<String, Object>> getOld() { return old; }
    public void setOld(List<Map<String, Object>> old) { this.old = old; }
    public List<String> getPkNames() { return pkNames; }
    public void setPkNames(List<String> pkNames) { this.pkNames = pkNames; }

    @JsonProperty("isDdl")
    public boolean isDdl() { return isDdl; }
    public void setDdl(boolean ddl) { isDdl = ddl; }
    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }
}
