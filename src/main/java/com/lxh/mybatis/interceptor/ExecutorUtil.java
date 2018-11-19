
package com.lxh.mybatis.interceptor;


import net.sf.jsqlparser.statement.update.Update;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.el.Expression;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 根据
 *
 * @author lxh
 */
public abstract class ExecutorUtil {

    private ExecutorUtil() {
    }

    private static final Logger logger = LoggerFactory.getLogger(ExecutorUtil.class);

    private static Field additionalParametersField;

    private static final List<ResultMapping> EMPTY_RESULTMAPPING = new ArrayList<ResultMapping>(0);


    static {
        try {
            additionalParametersField = BoundSql.class.getDeclaredField("additionalParameters");
            additionalParametersField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            try {
                throw new Exception("获取 BoundSql 属性 additionalParameters 失败: " + e, e);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * 获取 BoundSql 属性值 additionalParameters
     *
     * @param boundSql
     * @return
     */
    public static Map<String, Object> getAdditionalParameter(BoundSql boundSql) throws Exception {
        try {
            return (Map<String, Object>) additionalParametersField.get(boundSql);
        } catch (IllegalAccessException e) {
            throw new Exception("获取 BoundSql 属性值 additionalParameters 失败: " + e, e);
        }
    }

    /**
     * 执行自动生成的 count 查询
     *
     * @param executor
     * @param countMs
     * @param parameter
     * @param boundSql
     * @return
     * @throws SQLException
     */
    public static void queryIds(Executor executor, MappedStatement countMs,
                                Object parameter, BoundSql boundSql) throws Exception {
        //创建 count 查询的缓存 key

        // 如果可以查到Ids就一直更新，直到没有可更新的记录为止
        IterParam iterParam = new IterParam();
        iterParam.setMaxId("0");
        IdResultHandler resultHandler = new IdResultHandler();
        while (hasRecords(executor, countMs, parameter, null, resultHandler, boundSql, iterParam)) {
            logger.info("<=== ids: {}", resultHandler.ids);
            final String updateSql = getUpdateByIdsSql(boundSql.getSql(), resultHandler.ids);
            final MappedStatement updateMs = cloneMappedStatement(countMs, UUID.randomUUID().toString());
            final BoundSql updateBound = new BoundSql(countMs.getConfiguration(), updateSql, boundSql.getParameterMappings(), parameter);
            logger.info("===> sqlUpdateFor:{}", updateSql);
            try {
                executor.query(updateMs, parameter, RowBounds.DEFAULT, null, null, updateBound);
            } catch (SQLException e) {
                logger.error("OOO===> ",e);
            }
        }

    }

    /**
     * new bound sql
     *
     * @param ms
     * @param newMsId
     * @return
     */
    public static MappedStatement cloneMappedStatement(MappedStatement ms, String newMsId) {
        MappedStatement.Builder builder = new MappedStatement.Builder(ms.getConfiguration(), newMsId, ms.getSqlSource(), ms.getSqlCommandType());
        builder.resource(ms.getResource());
        builder.fetchSize(ms.getFetchSize());
        builder.statementType(ms.getStatementType());
        builder.keyGenerator(ms.getKeyGenerator());
        // set key properties
        ExecutorUtil.setKeyProperties(ms, builder);
        builder.timeout(ms.getTimeout());
        builder.parameterMap(ms.getParameterMap());
        //count查询返回值int
        List<ResultMap> resultMaps = new ArrayList<>();
        ResultMap resultMap = new ResultMap.Builder(ms.getConfiguration(), ms.getId(), Long.class, EMPTY_RESULTMAPPING).build();
        resultMaps.add(resultMap);
        builder.resultMaps(resultMaps);
        builder.resultSetType(ms.getResultSetType());
        builder.cache(ms.getCache());
        builder.flushCacheRequired(ms.isFlushCacheRequired());
        builder.useCache(ms.isUseCache());
        return builder.build();
    }

    /**
     * 翻译结果
     */
    static class IdResultHandler implements ResultHandler {
        List<String> ids = new ArrayList<>();

        public List<String> getIds() {
            return ids;
        }

        @Override
        public void handleResult(ResultContext context) {
            logger.info("<== {}", context.getResultObject());
            ids.add(context.getResultObject().toString());
        }
    }

    /**
     * 用来记录游标
     */
    static class IterParam {
        private boolean hasNext;
        private String maxId;

        public boolean isHasNext() {
            return hasNext;
        }

        public void setHasNext(boolean hasNext) {
            this.hasNext = hasNext;
        }

        public String getMaxId() {
            return maxId;
        }

        public void setMaxId(String maxId) {
            this.maxId = maxId;
        }
    }

    /**
     * 是否有需要更新的记录
     *
     * @param executor
     * @param countMs
     * @param parameter
     * @param countKey
     * @param resultHandler
     * @param boundSql
     * @param iterParam
     * @return
     */
    private static Boolean hasRecords(Executor executor, MappedStatement countMs, Object parameter, CacheKey countKey, IdResultHandler resultHandler, BoundSql boundSql, IterParam iterParam) throws Exception {
        try {
            String idsSql = getQueryByIdSql(boundSql.getSql(), iterParam.getMaxId());
            logger.info("===> idsSQL:{}", idsSql);
            Update update = getUpdate(boundSql.getSql());
            resultHandler.ids.clear();
            BoundSql countBoundSql = new BoundSql(countMs.getConfiguration(), idsSql, getParams(boundSql, update), parameter);
            //当使用动态 SQL 时，可能会产生临时的参数，这些参数需要手动设置到新的 BoundSql 中
            Map<String, Object> additionalParameters = getAdditionalParameter(boundSql);

            for (Map.Entry<String, Object> entry : additionalParameters.entrySet()) {
                countBoundSql.setAdditionalParameter(entry.getKey(), additionalParameters.get(entry.getKey()));
            }
            executor.query(countMs, parameter, RowBounds.DEFAULT, resultHandler, countKey, countBoundSql);

            boolean empty = CollectionUtils.isEmpty(resultHandler.ids);
            if (!empty) {
                int length = resultHandler.ids.size() - 1;
                resultHandler.ids.get(length);
                iterParam.setMaxId(resultHandler.ids.get(length));
            }
            return !empty;
        } catch (SQLException e) {
            throw new Exception(e);
        }
    }

    /**
     * 设置key properties
     *
     * @param ms
     * @param builder
     */
    public static void setKeyProperties(MappedStatement ms, MappedStatement.Builder builder) {
        if (ms.getKeyProperties() != null && ms.getKeyProperties().length != 0) {
            StringBuilder keyProperties = new StringBuilder();
            for (String keyProperty : ms.getKeyProperties()) {
                keyProperties.append(keyProperty).append(",");
            }
            keyProperties.delete(keyProperties.length() - 1, keyProperties.length());
            builder.keyProperty(keyProperties.toString());
        }
    }


    /**
     * 获取智能的countSql
     *
     * @param sql
     * @return
     */
    private static String getQueryByIdSql(String sql, String maxId) throws Exception {
        Update update = getUpdate(sql);
        if (null == update) {
            return "";
        }
        Expression where = update.getWhere();
        return "select id from " + update.getTables().get(0) + " where " + where + " and id > " + maxId + " order by id asc limit 10";
    }

    /**
     * update sql
     *
     * @param sql
     * @param ids
     * @return
     */
    private static String getUpdateByIdsSql(String sql, List<String> ids) {
        return sql + " and id >= " + ids.get(0) + " and id <= " + ids.get(ids.size() - 1);
    }

    /**
     * copy params
     *
     * @param boundSql
     * @param update
     * @return
     */
    private static List<ParameterMapping> getParams(BoundSql boundSql, Update update) {
        List<String> updateColums = new ArrayList<>();
        for (Expression expression : update.getColumns()) {
            updateColums.add(expression.toString());
        }
        List<ParameterMapping> newParams = new LinkedList<>();
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
            if (!updateColums.contains(parameterMapping.getProperty())) {
                newParams.add(parameterMapping);
            }
        }
        return newParams;
    }

    /**
     * get update sql
     *
     * @param sql
     * @return
     */
    private static Update getUpdate(String sql) throws Exception {
        Statement stmt = null;
        try {
            stmt = CCJSqlParserUtil.parse(sql);
        } catch (Exception e) {
            throw new Exception(e);
        }
        return (Update) stmt;
    }
}
