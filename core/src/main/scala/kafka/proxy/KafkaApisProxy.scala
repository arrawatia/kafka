/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.proxy

import java.lang.{Long => JLong, Short => JShort}

import kafka.admin.AdminClient
import kafka.common._
import kafka.network.RequestChannel.Response
import kafka.network._
import kafka.security.auth.Authorizer
import kafka.server.{KafkaApis, KafkaConfig}
import kafka.utils.{Logging, SystemTime}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol}
import org.apache.kafka.common.requests.{ApiVersionsResponse, MetadataRequest, MetadataResponse, ResponseHeader, ResponseSend}

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApisProxy(val requestChannel: RequestChannel,
                     val brokerId: Int,
                     val config: KafkaConfig,
                     val metrics: Metrics,
                     val authorizer: Option[Authorizer],
                     val proxyClient: AdminClient) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  // Store all the quota managers for each type of request


  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try {
      info("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
//        case ApiKeys.PRODUCE => handleProducerRequest(request)
//        case ApiKeys.FETCH => handleFetchRequest(request)
//        case ApiKeys.LIST_OFFSETS => handleOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
//        case ApiKeys.LEADER_AND_ISR => throw new KafkaException("ApiKeys.LEADER_AND_ISR not supported !")
//        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
//        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
//        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
//        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
//        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
//        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
//        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
//        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
//        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
//        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
//        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
//        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
//        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
//        case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
//        case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        if (request.requestObj != null) {
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }





  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body.asInstanceOf[MetadataRequest]
    val requestVersion = request.header.apiVersion()

    val response = proxyClient.sendAnyNode(ApiKeys.METADATA, metadataRequest)
    val responseBody = new MetadataResponse(response)
    info("Respose : " + responseBody.topicMetadata())
//    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
//      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    val responseHeader = new ResponseHeader(request.header.correlationId)

//    val responseBody = new MetadataResponse(
//      brokers.map(_.getNode(request.securityProtocol)).asJava,
//      metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
//      completeTopicMetadata.asJava,
//      requestVersion
//    )
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }


  def handleApiVersionsRequest(request: RequestChannel.Request) {
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
      ApiVersionsResponse.apiVersionsResponse
    else
      ApiVersionsResponse.fromError(Errors.UNSUPPORTED_VERSION)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def close() {
    info("Shutdown complete.")
  }



}
