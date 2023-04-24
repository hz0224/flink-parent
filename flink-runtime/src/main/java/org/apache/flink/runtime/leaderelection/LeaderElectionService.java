/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import javax.annotation.Nonnull;

import java.util.UUID;

/**
 * // TODO_MA 注释： 一种服务接口，允许在一组竞争者中选举一位领导者。
 * Interface for a service which allows to elect a leader among a group of contenders.
 *
 * // TODO_MA 注释： 使用此服务之前，必须先调用start方法将其启动。
 * // TODO_MA 注释： 启动方法将竞争者作为参数。如果有多个竞争者，那么每个竞争者都必须实例化其自己的领导者选举服务。
 * <p>Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has to
 * instantiate its own leader election service.
 *
 * // TODO_MA 注释： 授予竞争者领导权后，他必须通过调用方法{@link #confirmLeadership（UUID，String）}
 * // TODO_MA 注释： 来确认收到的领导者会话ID 。这将通知领导者选举服务，竞争者已接受指定的领导者，
 * // TODO_MA 注释： 并且领导者会话ID和领导者地址现在可以发布用于领导者检索服务。
 * <p>Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method {@link #confirmLeadership(UUID, String)}. This will notify the leader
 * election service, that the contender has accepted the leadership specified and that the leader
 * session id as well as the leader address can now be published for leader retrieval services.
 */
public interface LeaderElectionService {

    /**
     * Starts the leader election service. This method can only be called once.
     *
     * @param contender LeaderContender which applies for the leadership
     * @throws Exception
     */
    void start(LeaderContender contender) throws Exception;

    /**
     * Stops the leader election service.
     *
     * @throws Exception
     */
    void stop() throws Exception;

    /**
     * Confirms that the {@link LeaderContender} has accepted the leadership identified by the given
     * leader session id. It also publishes the leader address under which the leader is reachable.
     *
     * <p>The rational behind this method is to establish an order between setting the new leader
     * session ID in the {@link LeaderContender} and publishing the new leader session ID as well as
     * the leader address to the leader retrieval services.
     *
     * @param leaderSessionID The new leader session ID
     * @param leaderAddress The address of the new leader
     */
    void confirmLeadership(UUID leaderSessionID, String leaderAddress);

    /**
     * Returns true if the {@link LeaderContender} with which the service has been started owns
     * currently the leadership under the given leader session id.
     *
     * @param leaderSessionId identifying the current leader
     * @return true if the associated {@link LeaderContender} is the leader, otherwise false
     */
    boolean hasLeadership(@Nonnull UUID leaderSessionId);
}
