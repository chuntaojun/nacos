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

package com.alibaba.nacos.naming.consistency.persistent.raft;

/**
 * Leader election finished event.
 *
 * @author pbting
 * @date 2019-07-01 8:25 PM
 * @deprecated will remove in 1.4.x
 */
@Deprecated
public class LeaderElectFinishedEvent extends BaseRaftEvent {
    
    public LeaderElectFinishedEvent(Object source, RaftPeer raftPeer, RaftPeer local) {
        super(source, raftPeer, local);
    }
    
}
