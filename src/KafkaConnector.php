<?php

declare(strict_types=1);

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Facades\Log;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class KafkaConnector implements ConnectorInterface
{

    /**
     * Connector method for kafka queue
     *
     * @param array $config
     * @return KafkaQueue
     */
    public function connect(array $config): KafkaQueue
    {
        $conf = new Conf();

        $conf->set('bootstrap.servers', $config['bootstrap.servers']);
        $conf->set('security.protocol', $config['security.protocol']);

        $conf->set('socket.timeout.ms', "50");
        $conf->set('queue.buffering.max.messages', "1000");
        $conf->set('max.in.flight.requests.per.connection', "1");

        $conf->set('sasl.mechanism', $config['sasl.mechanism']);
        $conf->set('sasl.username', $config['sasl.username']);
        $conf->set('sasl.password', $config['sasl.password']);

        $conf->setDrMsgCb(
            function (Producer $producer, Message $message): void {
                if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw new Exception($message->errstr());
                }
            }
        );

        $conf->set('log_level', (string)LOG_DEBUG);
        $conf->set('debug', 'all');
        $conf->setLogCb(
            function (Producer $producer, int $level, string $facility, string $message): void {
                Log::debug('kafka debug', ['message' => $message]);
            }
        );

        $conf->set('group.id', $config['group.id']);
        $conf->set('auto.offset.reset', $config['auto.offset.reset']);

        $consumer = new KafkaConsumer($conf);
        $producer = new Producer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
