package com.lxh.mybatis.interceptor;

import com.github.pagehelper.cache.Cache;
import com.github.pagehelper.cache.CacheFactory;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @author lxh
 */
@Intercepts({@Signature(
        type = Executor.class,
        method = "update",
        args = {MappedStatement.class, Object.class})})
public class NewsInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(NewsInterceptor.class);
    private String idSuffix = "_ID";

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object[] args = invocation.getArgs();
        MappedStatement ms = (MappedStatement) args[0];
        Object parameter = args[1];
        Executor executor = (Executor) invocation.getTarget();
        BoundSql boundSql;
        boundSql = ms.getBoundSql(parameter);
        if(!boundSql.getSql().contains("mesg_news_record") || SqlCommandType.UPDATE.compareTo(ms.getSqlCommandType()) != 0) {
            return invocation.proceed();
        }
        // 如果是关于mesg_news_record的更新就处理一下，不执行原来的SQL，改成执行我们想要的SQL
        myUpdateInterceptor(ms, boundSql, executor, parameter);
        return -1;
    }

    /**
     * 拦截一下
     *
     * @param ms
     * @param boundSql
     * @param executor
     * @param parameter
     * @throws SQLException
     */
    private void myUpdateInterceptor(MappedStatement ms, BoundSql boundSql, Executor executor, Object parameter) throws Exception {

        String idMsId = ms.getId() + idSuffix;
        MappedStatement selectIdStatement = ExecutorUtil.cloneMappedStatement(ms, idMsId);
        ExecutorUtil.queryIds(executor, selectIdStatement,
                parameter, boundSql);
    }


    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {


    }
}
