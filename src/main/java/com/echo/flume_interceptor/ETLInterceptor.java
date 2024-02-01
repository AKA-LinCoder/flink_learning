package com.echo.flume_interceptor;

import com.echo.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

///flume拦截器
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1 获取body这的数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        //2 判断是不是合法的json
        //3 是 return event,不是 return null
        if(JSONUtil.isJSONValidate(log)){
            return  event;
        }else{
            return  null;
        }

    }

    /// 删除有问题的数据，保留没有问题的数据
    @Override
    public List<Event> intercept(List<Event> list) {

        //1 迭代器实现
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            Event event = iterator.next();
            if(intercept(event)==null){
                iterator.remove();
            }
        }
        return list;
        //2
//        list.removeIf(event -> intercept(event) == null);
//        return list;

//        //3 for 循环实现
//        List<Event> temp = list;
//        for (int i = 0;i<temp.size();i++) {
//            Event event = temp.get(i);
//            if(intercept(event)==null){
//                list.remove(i);
//            }
//        }
//        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor() ;
        }

        @Override
        public void configure(Context context) {

        }
    }
}
