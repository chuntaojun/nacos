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
package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.core.utils.ClassUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author nacos
 */
public class HealthCheckReactor {

    private static final ScheduledExecutorService EXECUTOR;

    static {

        int processorCount = Runtime.getRuntime().availableProcessors();
        EXECUTOR = ExecutorFactory.newScheduledExecutorService(
                ClassUtils.getPackageName(HealthCheckReactor.class),
                processorCount <= 1 ? 1 : processorCount / 2,
                new NameThreadFactory("com.alibaba.nacos.naming.health"));
    }

    public static ScheduledFuture<?> scheduleCheck(HealthCheckTask task) {
        task.setStartTime(System.currentTimeMillis());
        return EXECUTOR.schedule(task, task.getCheckRTNormalized(), TimeUnit.MILLISECONDS);
    }

    public static ScheduledFuture<?> scheduleNow(Runnable task) {
        return EXECUTOR.schedule(task, 0, TimeUnit.MILLISECONDS);
    }
}
