/*
 *  Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.nacos.config.server.service.watch;

import com.alibaba.nacos.common.utils.ConcurrentHashSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public final class WatchClientManager {
    
    private static final String LINK_STR = "@@";
    
    private final Map<String, Map<String, Set<WatchClient>>> config2ClientMap = new ConcurrentHashMap<>(128);
    
    private final Map<String, Set<WatchClient>> ip2ClientMap = new ConcurrentHashMap<>(128);
    
    private final Map<String, Long> retainIps = new ConcurrentHashMap<>(128);
    
    public Map<String, Long> getRetainIps() {
        return retainIps;
    }
    
    public int size() {
        int clientSize = 0;
        for (Map.Entry<String, Set<WatchClient>> entry : ip2ClientMap.entrySet()) {
            clientSize += entry.getValue().size();
        }
        return clientSize;
    }
    
    public void addWatchClient(final WatchClient client) {
        ip2ClientMap.computeIfAbsent(client.getAddress(), address -> new ConcurrentHashSet<>());
        ip2ClientMap.get(client.getAddress()).add(client);
        config2ClientMap.computeIfAbsent(client.getNamespace(), namespace -> new ConcurrentHashMap<>(128));
        Map<String, Set<WatchClient>> clients = config2ClientMap.get(client.getNamespace());
        client.getWatchKey().forEach((key, md5sum) -> {
            clients.computeIfAbsent(key, groupIDAndDataID -> new ConcurrentHashSet<>());
            clients.get(key).add(client);
        });
    }
    
    public Set<WatchClient> findClientByAddress(final String address) {
        return ip2ClientMap.getOrDefault(address, Collections.emptySet());
    }
    
    public Set<WatchClient> findClientsByGroupKey(final String namespace, final String groupID, final String dataID) {
        final String key = groupID + LINK_STR + dataID;
        Map<String, Set<WatchClient>> config2Clients = config2ClientMap.getOrDefault(namespace, Collections.emptyMap());
        return config2Clients.getOrDefault(key, Collections.emptySet());
    }
    
    /**
     * Traversal processing listener client
     *
     * @param clientConsumer {@link Consumer}
     */
    public void forEach(Consumer<WatchClient> clientConsumer) {
        ip2ClientMap.forEach((address, clients) -> clients.forEach(clientConsumer));
    }
    
    /**
     * remove watch client
     *
     * @param client {@link WatchClient}
     */
    public void removeWatchClient(final WatchClient client) {
        ip2ClientMap.getOrDefault(client.getAddress(), Collections.emptySet()).remove(client);
        config2ClientMap
                .forEach((s, stringSetMap) -> stringSetMap.forEach((s1, watchClients) -> watchClients.remove(client)));
    }
}