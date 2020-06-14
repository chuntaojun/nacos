/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.test.naming;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.LessorCenter;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.mock.web.MockServletContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class LessorCenterTest {

	private static DistroMapper mapper;

	private static GlobalConfig config;

	private static int service_num = 100;
	private static int instance_num = 1000;

	private static Collection<Member> members;

	static {
		try {

			ApplicationUtils.injectEnvironment(new StandardEnvironment());
			members = Collections
					.singletonList(Member.builder().ip("1.1.1.1").port(888).build());
			config = new GlobalConfig();

			ApplicationUtils.setLocalAddress("1.1.1.1:8848");
			mapper = new DistroMapper(new ServerMemberManager(new MockServletContext()) {
				@Override
				public Collection<Member> allMembers() {
					return members;
				}
			}, new SwitchDomain());
			mapper.init();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	static final ScheduledExecutorService EXECUTOR_SERVICE = Executors
			.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() << 2);
	private static final Map<String, Map<String, Instance>> STORAGE = new ConcurrentHashMap<>();
	private static final Set<Instance> ALREADY_UN_HEALTH = new ConcurrentHashSet<>();

	private static final Set<Instance> ALREADY_REMOVE = new ConcurrentHashSet<>();

	private static final String CLUSTER_NAME = "DEFAULT_CLUSTER";

	private static final LessorCenter LESSOR_CENTER = new LessorCenter(1, 16, null,
			mapper, config) {
		@Override
		public BiConsumer<Collection<Instance>, Boolean> createConsumer() {
			return (instances, isUnHealth) -> {
				List<Instance> failedInstances = new ArrayList<>();
				for (Instance instance : instances) {
					final String serviceName = instance.getServiceName();
					if (normal.get(serviceName).contains(instance)) {
						Assert.fail();
					} else {
						if (isUnHealth) {
							ALREADY_UN_HEALTH.add(instance);
						} else {
							ALREADY_REMOVE.add(instance);
						}
					}
				}
				System.out.println(
						"isUnHealth : " + isUnHealth + ", failed : " + failedInstances.size() + ", total : " + instances.size() + ", error rate is several percent : " + (
								(failedInstances.size() * 1.0) / instances.size()) + ", total unHealth instances" + ALREADY_UN_HEALTH.size() + ", total remove instances : " + ALREADY_REMOVE.size());
			};
		}
	};

	private static final Map<String, Set<Instance>> slow = new ConcurrentHashMap<>();
	private static final Map<String, Set<Instance>> normal = new ConcurrentHashMap<>();

	@BeforeClass
	public static void beforeClass() {
		initData();
		registerInstance();

		System.out.println("slow instance = " + slow.values().stream().map(Set::size)
				.reduce(Integer::sum));

		System.out.println("normal instance = " + normal.values().stream().map(Set::size)
				.reduce(Integer::sum));
	}

	@Test
	public void test_object_expire() {
		normal.forEach((serviceName, instances) -> instances.forEach(
				instance -> sendBeat(serviceName, instance, EXECUTOR_SERVICE, true)));
		//		slow.forEach((serviceName, instance) -> sendBeat(serviceName, instance, slowExecutor, false));

		int slowInstance = slow.values().stream().map(Set::size)
				.reduce(Integer::sum).get();
		while (ALREADY_UN_HEALTH.size() != slowInstance) {
			ThreadUtils.sleep(2_000L);
		}

		while (ALREADY_REMOVE.size() != slowInstance) {
			ThreadUtils.sleep(2_000L);
		}

		ThreadUtils.sleep(30_000L);
		if (ALREADY_REMOVE.size() != slowInstance || ALREADY_UN_HEALTH.size() != slowInstance) {
			Assert.fail();
		}
	}

	private static void initData() {
		AtomicInteger count = new AtomicInteger(0);
		for (; ; ) {
			final String serviceName = NamingUtils
					.getGroupedName(NamingBase.randomDomainName(),
							Constants.DEFAULT_GROUP);
			STORAGE.computeIfAbsent(serviceName, key -> {
				Map<String, Instance> map = new ConcurrentHashMap<>();
				count.incrementAndGet();
				return map;
			});
			if (count.get() == service_num) {
				return;
			}
		}
	}

	private static void registerInstance() {
		CountDownLatch latch = new CountDownLatch(service_num);
		final Object monitor = new Object();
		final long startTime = System.currentTimeMillis();
		Set<String> ipSet = new HashSet<>();
		STORAGE.forEach((serviceName, map) -> EXECUTOR_SERVICE.execute(() -> {
			try {
				for (int i = 0; i < instance_num; i++) {
					String ip;

					synchronized (monitor) {
						for (; ; ) {
							ip = getRandomIp();
							if (!ipSet.contains(ip)) {
								break;
							}
						}
						ipSet.add(ip);
					}

					final int port = 12345;
					final Instance instance = Instance.builder().serviceName(serviceName).ip(ip).port(port).clusterName(CLUSTER_NAME).ephemeral(true)
							.build();
					if (i % 7 == 0) {
						slow.computeIfAbsent(serviceName, s -> new HashSet<>(1024));
						slow.get(serviceName).add(instance);
					}
					else {
						normal.computeIfAbsent(serviceName, s -> new HashSet<>(1024));
						normal.get(serviceName).add(instance);
					}
					map.put(instance.getDatumKey(), instance);
					LESSOR_CENTER.register(instance);
				}
			} catch (Throwable ex) {
				ex.printStackTrace();
				Assert.fail();
			} finally {
				latch.countDown();
			}
		}));

		ThreadUtils.latchAwait(latch);
		System.out.println(
				"Instance creation time : " + (System.currentTimeMillis() - startTime)
						+ " Ms");
		Assert.assertEquals(service_num * instance_num, ipSet.size());
	}

	private static void sendBeat(String serviceName, Instance instance,
			ScheduledExecutorService executorService, boolean isNormal) {
		final RsInfo rsInfo = RsInfo.builder().serviceName(serviceName)
				.cluster(instance.getClusterName()).ip(instance.getIp())
				.port(instance.getPort()).build();
		onBeat(rsInfo);
		long delay = 5;
		if (!isNormal) {
			delay += ThreadLocalRandom.current().nextInt(5);
		}
		executorService.schedule(
				() -> sendBeat(serviceName, instance, executorService, isNormal), delay,
				TimeUnit.SECONDS);
	}

	private static void onBeat(RsInfo rsInfo) {
		final String serviceName = rsInfo.getServiceName();
		final String ip = rsInfo.getIp();
		final int port = rsInfo.getPort();
		final String clusterName = rsInfo.getCluster();
		final String key = Instance.buildDatumKey(ip, port, clusterName);
		Map<String, Instance> map = STORAGE.get(serviceName);
		if (map.containsKey(key)) {
			map.get(key).setLastBeat(System.currentTimeMillis());
		}
		else {
			Instance instance = Instance.builder().ip(ip).port(port)
					.clusterName(clusterName).lastBeat(System.currentTimeMillis())
					.build();
			LESSOR_CENTER.register(instance);
			map.put(key, instance);
			System.err.println("The instance does not exist");
		}
	}

	// copy from https://blog.csdn.net/zhengxiongwei/article/details/78486146

	public static String getRandomIp() {

		// ip范围
		int[][] range = { { 607649792, 608174079 }, // 36.56.0.0-36.63.255.255
				{ 1038614528, 1039007743 }, // 61.232.0.0-61.237.255.255
				{ 1783627776, 1784676351 }, // 106.80.0.0-106.95.255.255
				{ 2035023872, 2035154943 }, // 121.76.0.0-121.77.255.255
				{ 2078801920, 2079064063 }, // 123.232.0.0-123.235.255.255
				{ -1950089216, -1948778497 }, // 139.196.0.0-139.215.255.255
				{ -1425539072, -1425014785 }, // 171.8.0.0-171.15.255.255
				{ -1236271104, -1235419137 }, // 182.80.0.0-182.92.255.255
				{ -770113536, -768606209 }, // 210.25.0.0-210.47.255.255
				{ -569376768, -564133889 }, // 222.16.0.0-222.95.255.255
		};

		Random random = ThreadLocalRandom.current();
		int index = random.nextInt(10);
		return num2ip(
				range[index][0] + random.nextInt(range[index][1] - range[index][0]));
	}

	/*
	 * 将十进制转换成IP地址
	 */
	public static String num2ip(int ip) {
		int[] b = new int[4];
		String ipStr = "";
		b[0] = (ip >> 24) & 0xff;
		b[1] = (ip >> 16) & 0xff;
		b[2] = (ip >> 8) & 0xff;
		b[3] = ip & 0xff;
		ipStr = b[0] + "." + b[1] + "." + b[2] + "." + b[3];

		return ipStr;
	}

}