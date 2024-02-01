package com.echo.utils;

import com.alibaba.fastjson.JSONObject;

public class JSONUtil {
    //校验json格式是否正确
    public static boolean isJSONValidate(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        }catch (Exception e){
            return false;
        }
    }

}
