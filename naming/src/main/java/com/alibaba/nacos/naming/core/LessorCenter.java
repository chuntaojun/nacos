/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.core.cluster.MemberChangeEvent;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.utils.ApplicationUtils;
import com.alibaba.nacos.core.utils.HashUtils;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class LessorCenter implements MemberChangeListener {

	private final Lessor[] ring;
	private final ServiceManager serviceManager;
	private final DistroMapper mapper;
	private final GlobalConfig config;

	private final Map<String, MpscUnboundedArrayQueue<Instance>> waitSelfResponse = new NonBlockingHashMap<>();

	private AtomicBoolean shutdown = new AtomicBoolean(false);

	public LessorCenter(int size, int slot, ServiceManager serviceManager, DistroMapper mapper, GlobalConfig config) {
		this.ring = new Lessor[size];
		this.serviceManager = serviceManager;
		this.mapper = mapper;
		this.config = config;
		init(size, slot);
	}

	private void init(int size, int slot) {
		for (int i = 0; i < size; i ++) {
			ring[i] = Lessor.createLessor(slot , Duration.ofSeconds(1), createConsumer());
		}
	}

	public void register(Instance instance) {
		final String serviceName = instance.getServiceName();
		if (mapper.responsible(serviceName)) {
			add(instance);
		} else {
			waitSelfResponse.computeIfAbsent(serviceName, s -> new MpscUnboundedArrayQueue<>(128));
			waitSelfResponse.get(serviceName).add(instance);
		}
	}

	private void add(Instance instance) {
		final String key = instance.getDatumKey();
		final Lessor lessor = HashUtils.mapper(ring, key);
		lessor.addItem(instance, instance.getInstanceHeartBeatTimeOut() * 1000000L);
	}

	public BiConsumer<Collection<Instance>, Boolean> createConsumer() {
		return (instances, isUnHealth) -> {
			Map<String, Map<String, List<Instance>>> waitRemove = parse(instances);
			if (isUnHealth) {
				onUnHealth(waitRemove);
			} else {
				if (!config.isExpireInstance()) {
					return;
				}
				onDelete(waitRemove);
			}
		};
	}

	private void onUnHealth(Map<String, Map<String, List<Instance>>> instances) {
		instances.forEach((namespaceId, serviceMap) -> serviceMap.forEach((serviceName, ips) -> {
			if (!mapper.responsible(serviceName)) {
				return;
			}
			ips.forEach(instance -> {
				Loggers.EVT_LOG.info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
						instance.getIp(), instance.getPort(), instance.getClusterName(), serviceName,
						UtilsAndCommons.LOCALHOST_SITE, instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
				ApplicationUtils
						.publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
			});
			Service service = serviceManager.getService(namespaceId, serviceName);
			service.getPushService().serviceChanged(service);
		}));
	}

	private void onDelete(Map<String, Map<String, List<Instance>>> instances) {
		instances.forEach((namespaceId, serviceMap) -> serviceMap.forEach((serviceName, ips) -> {
			if (!mapper.responsible(serviceName)) {
				return;
			}
			try {
				serviceManager.removeInstance(namespaceId, serviceName, true, ips.toArray(new Instance[0]));
				ips.forEach(instance -> Loggers.EVT_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", serviceName, instance.toString()));
			} catch (NacosException e) {
				Loggers.EVT_LOG.error("Instance culls out an exception : {}",
						ExceptionUtil.getStackTrace(e));
			}
		}));
	}

	private static Map<String, Map<String, List<Instance>>> parse(Collection<Instance> instances) {
		Map<String, Map<String, List<Instance>>> waitRemove = new HashMap<>();
		instances.forEach(instance -> {
			final String namespace = instance.getTenant();
			final String serviceName = instance.getServiceName();
			waitRemove.computeIfAbsent(namespace, s -> new HashMap<>());
			waitRemove.get(namespace).computeIfAbsent(serviceName, s -> new ArrayList<>());
			waitRemove.get(namespace).get(serviceName).add(instance);
		});
		return waitRemove;
	}

	@Override
	public void onEvent(MemberChangeEvent event) {
		GlobalExecutor.submit(() -> {
			if (waitSelfResponse.isEmpty()) {
				return;
			}
			List<String> waitRemove = new ArrayList<>();
			waitSelfResponse.forEach((name, instances) -> {
				if (mapper.responsible(name)) {
					waitRemove.add(name);
					instances.forEach(this::add);
				}
			});
			waitRemove.forEach(waitSelfResponse::remove);
		});
	}

	public void shutdown() {
		if (!shutdown.compareAndSet(false, true)) {
			return;
		}
		for (Lessor lessor : ring) {
			lessor.stop();
		}
	}
}
