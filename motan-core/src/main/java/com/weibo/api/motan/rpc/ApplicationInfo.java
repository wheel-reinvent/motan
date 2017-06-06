/*
 *  Copyright 2009-2016 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.api.motan.rpc;

import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.common.URLParamType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Local app info.
 *
 * @author fishermen
 * @version V1.0 created at: 2013-6-20
 */

public class ApplicationInfo {

    public static final String STATISTIC = "statisitic";
    public static final ConcurrentMap<String, Application> applications = new ConcurrentHashMap<String, Application>();
    private static String CLIENT = "-client";

    // 从应用map中获取url对应的应用
    public static Application getApplication(URL url) {
        Application application = applications.get(url.getPath());
        // 如果url对应的应用不存在并且url中key为nodeType的参数的值是referer则应用的类型是client,url的路径拼接上client后缀
        if (application == null && MotanConstants.NODE_TYPE_REFERER.equals(url.getParameter(URLParamType.nodeType.getName()))) {
            application = applications.get(url.getPath() + CLIENT);
            // 如果url-client对应的应用不存在
            if(application == null){
                // 根据应用的名称和模块的名称创建新的应用并放入应用字典中
                String app = url.getParameter(URLParamType.application.getName(), URLParamType.application.getValue()) + CLIENT;
                String module = url.getParameter(URLParamType.module.getName(), URLParamType.module.getValue()) + CLIENT;

                applications.putIfAbsent(url.getPath() + CLIENT, new Application(app, module));
                application = applications.get(url.getPath() + CLIENT);
            }
        }
        return application;
    }

    // 添加url对应的服务(应用)到应用字典中(如果已经存在不做处理)
    public static void addService(URL url) {
        Application application = applications.get(url.getPath());
        if (application == null) {
            String app = url.getParameter(URLParamType.application.getName(), URLParamType.application.getValue());
            String module = url.getParameter(URLParamType.module.getName(), URLParamType.module.getValue());

            applications.putIfAbsent(url.getPath(), new Application(app, module));
        }
    }
}
