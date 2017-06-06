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

package com.weibo.api.motan.registry.support.command;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.weibo.api.motan.registry.NotifyListener;
import com.weibo.api.motan.registry.support.FailbackRegistry;
import com.weibo.api.motan.rpc.URL;
import com.weibo.api.motan.util.LoggerUtil;

public abstract class CommandFailbackRegistry extends FailbackRegistry {

    private ConcurrentHashMap<URL, CommandServiceManager> commandManagerMap;

    public CommandFailbackRegistry(URL url) {
        super(url);
        commandManagerMap = new ConcurrentHashMap<URL, CommandServiceManager>();
        LoggerUtil.info("CommandFailbackRegistry init. url: " + url.toSimpleString());
    }

    /**
     * 订阅
     */
    @Override
    protected void doSubscribe(URL url, final NotifyListener listener) {
        LoggerUtil.info("CommandFailbackRegistry subscribe. url: " + url.toSimpleString());
        // 获取URL的拷贝
        URL urlCopy = url.createCopy();
        // 获取URL对应的命令服务管理器，如果不存在则创建
        CommandServiceManager manager = getCommandServiceManager(urlCopy);
        // 管理器添加通知监听器
        manager.addNotifyListener(listener);

        // 订阅服务
        subscribeService(urlCopy, manager);
        // 订阅命令
        subscribeCommand(urlCopy, manager);

        // 做发现
        List<URL> urls = doDiscover(urlCopy);
        // 对发现的url遍历通知
        if (urls != null && urls.size() > 0) {
            this.notify(urlCopy, listener, urls);
        }
    }

    /**
     * 取消订阅
     */
    @Override
    protected void doUnsubscribe(URL url, NotifyListener listener) {
        LoggerUtil.info("CommandFailbackRegistry unsubscribe. url: " + url.toSimpleString());
        // 获取URL的拷贝
        URL urlCopy = url.createCopy();
        // 获取URL对应的命令服务管理器
        CommandServiceManager manager = commandManagerMap.get(urlCopy);

        // 移除管理器中的通知监听器
        manager.removeNotifyListener(listener);
        // 取消订阅服务
        unsubscribeService(urlCopy, manager);
        // 取消订阅命令
        unsubscribeCommand(urlCopy, manager);

    }

    /**
     * 发现
     */
    @Override
    protected List<URL> doDiscover(URL url) {
        LoggerUtil.info("CommandFailbackRegistry discover. url: " + url.toSimpleString());
        List<URL> finalResult;
        // 获取URL的拷贝
        URL urlCopy = url.createCopy();
        // 获取命令
        String commandStr = discoverCommand(urlCopy);
        RpcCommand rpcCommand = null;
        if (StringUtils.isNotEmpty(commandStr)) {
            rpcCommand = RpcCommandUtil.stringToCommand(commandStr);

        }

        LoggerUtil.info("CommandFailbackRegistry discover command. commandStr: " + commandStr + ", rpccommand "
                + (rpcCommand == null ? "is null." : "is not null."));

        if (rpcCommand != null) {
            //指令不为空,使用命令服务管理器进行发现
            // 排序
            rpcCommand.sort();
            CommandServiceManager manager = getCommandServiceManager(urlCopy);
            finalResult = manager.discoverServiceWithCommand(urlCopy, new HashMap<String, Integer>(), rpcCommand);

            // 在subscribeCommon时，可能订阅完马上就notify，导致首次notify指令时，可能还有其他service没有完成订阅，
            // 此处先对manager更新指令，避免首次订阅无效的问题。
            manager.setCommandCache(commandStr);
        } else {
            //指令为空,调用子类发现服务的实现
            finalResult = discoverService(urlCopy);
        }

        LoggerUtil.info("CommandFailbackRegistry discover size: " + finalResult.size() + ", result:" + finalResult.toString());

        return finalResult;
    }

    public List<URL> commandPreview(URL url, RpcCommand rpcCommand, String previewIP) {
        List<URL> finalResult;
        URL urlCopy = url.createCopy();

        if (rpcCommand != null) {
            CommandServiceManager manager = getCommandServiceManager(urlCopy);
            finalResult = manager.discoverServiceWithCommand(urlCopy, new HashMap<String, Integer>(), rpcCommand, previewIP);
        } else {
            finalResult = discoverService(urlCopy);
        }

        return finalResult;
    }

    private CommandServiceManager getCommandServiceManager(URL urlCopy) {
        // 首先从成员变量commandManagerMap中获取url对应的管理器，如果不存在则创建并放入成员变量commandManagerMap中
        CommandServiceManager manager = commandManagerMap.get(urlCopy);
        if (manager == null) {
            manager = new CommandServiceManager(urlCopy);
            manager.setRegistry(this);
            CommandServiceManager manager1 = commandManagerMap.putIfAbsent(urlCopy, manager);
            if (manager1 != null) manager = manager1;
        }
        return manager;
    }

    // for UnitTest
    public ConcurrentHashMap<URL, CommandServiceManager> getCommandManagerMap() {
        return commandManagerMap;
    }

    protected abstract void subscribeService(URL url, ServiceListener listener);

    protected abstract void subscribeCommand(URL url, CommandListener listener);

    protected abstract void unsubscribeService(URL url, ServiceListener listener);

    protected abstract void unsubscribeCommand(URL url, CommandListener listener);

    protected abstract List<URL> discoverService(URL url);

    protected abstract String discoverCommand(URL url);

}
