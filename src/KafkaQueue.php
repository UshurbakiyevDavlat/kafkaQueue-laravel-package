<?php

declare(strict_types=1);

namespace Kafka;

use Exception;
use Illuminate\Queue\Queue;
use RdKafka\TopicConf;

class KafkaQueue extends Queue implements \Illuminate\Contracts\Queue\Queue
{
    private const REQ_TIMEOUT = 8000;
    private const MESSAGE_TIMEOUT = 30000;
    private const REQ_ACKS = -1;
    private const PARTITION = 0;
    private const CONSUME_TIME = 120 * 1000;

    public function __construct(private $producer, private $consumer)
    {
    }

    public function setConnection()
    {
    }

    public function size($queue = null)
    {
    }

    /**
     * Producer push method
     *
     * @param $job
     * @param $data
     * @param $queue
     * @return void
     */
    public function push($job, $data = '', $queue = null): void
    {
        $topic = $queue ?? env('KAFKA_TOPIC');
        $topicConf = new TopicConf();
        $topicConf->set('message.timeout.ms', (string)self::MESSAGE_TIMEOUT);
        $topicConf->set('request.required.acks', (string)self::REQ_ACKS);
        $topicConf->set('request.timeout.ms', (string)self::REQ_TIMEOUT);

        $topic = $this->producer->newTopic($topic, $topicConf);
        $topic->produce(RD_KAFKA_PARTITION_UA, self::PARTITION, serialize($job));
        $this->producer->flush(static::REQ_TIMEOUT);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
    }


    /**
     * Consumer pop method
     *
     * @param $queue
     * @return void
     * @throws Exception
     */
    public function pop($queue = null): void
    {
        $topic = $queue ?? env('KAFKA_TOPIC');

        $this->consumer->subscribe([$topic]);
        $message = $this->consumer->consume(self::CONSUME_TIME);

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $job = unserialize($message->payload);
                $job->handle();
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                var_dump("No more messages; will wait for more\n");
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                var_dump("Timed out\n");
                break;
            default:
                throw new Exception($message->errstr(), $message->err);
        }
    }
}
