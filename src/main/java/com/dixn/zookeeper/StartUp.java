package com.dixn.zookeeper;

import com.dixn.zookeeper.client.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Order(value = 1)
@Slf4j
public class StartUp implements ApplicationRunner {

	@Autowired
	ZkClient zkClient;
	/**
	 * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
	 */
	ExecutorService exec = Executors.newFixedThreadPool(2);

	@Override
	public void run(ApplicationArguments var1) {
		String znode = "/" + "services";
		watchPath(znode,(client, event) ->{
			callProcess(client, event);
		});
	}

	/**
	 * 监听数据节点的变化情况
	 * @param watchPath
	 * @param listener
	 */
	public void watchPath(String watchPath, TreeCacheListener listener){
		TreeCache cache = new TreeCache(zkClient.getClient(), watchPath);
		cache.getListenable().addListener(listener,exec);
		try {
			cache.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void callProcess(CuratorFramework client, TreeCacheEvent event) {
		log.info("event:" + event.getType() +
				" |path:" + (null != event.getData() ? event.getData().getPath() : null));

		if(event.getData()!=null && event.getData().getData()!=null){
			log.info("发生变化的节点内容为：" + new String(event.getData().getData()));
		}
	}
}
