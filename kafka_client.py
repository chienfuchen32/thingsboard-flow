from kafka import KafkaConsumer

import transport_pb2

TOPIC = 'tb.rule-engine'
GROUP_ID = 'thingsboard-flow'
BOOTSTRAP_SERVERS = 'HOST:9092'
consumer = KafkaConsumer(TOPIC,
                         group_id=GROUP_ID,
                         bootstrap_servers=BOO)

for msg in consumer:
    rule_engine_msg = transport_pb2.ToRuleEngineMsg()
    rule_engine_msg.ParseFromString(msg.value)
    '''
    toDeviceActorMsg {
      sessionInfo {
        nodeId: "tb-mqtt-transport-764674bdf6-45fcf"
        sessionIdMSB: 8852251556884532417
        sessionIdLSB: -5466539027407923240
        tenantIdMSB: -8599705949809733140
        tenantIdLSB: -5634867372444484078
        deviceIdMSB: 6499731035068043756
        deviceIdLSB: -7442614560998757628
      }
      postTelemetry {
        tsKvList {
          ts: 1645799746418
          kv {
            key: "key1"
            type: LONG_V
            long_v: 360
          }
        }
      }
    }
    rule_engine_msg.toDeviceActorMsg.postTelemetry.tsKvList[0].kv[0].key
    rule_engine_msg.toDeviceActorMsg.sessionInfo.tenantIdLSB
    rule_engine_msg.toDeviceActorMsg.sessionInfo.deviceIdLSB
    '''
