# frozen_string_literal: true

require 'aws-sdk'

class AwsChicagoCrimesConsumers
  def initialize
    @client = Aws::Kinesis::Client.new(region: 'ap-northeast-1')
  end

  def consumeAndAlert(streamName)
    shards = getShards(streamName)
    puts("Got the Shards. Total Shard = #{shards.size}")
    if shards.size > 0
      puts("Getting Data from Shard- #{shards.first.shard_id}")
      processShard(shards.first, streamName)
    end
  end

  private

  attr_reader :client

  def getShards(streamName)
    shards = []
    exclusiveStartShardId = nil
    begin
      result = client.describe_stream(stream_name: streamName, exclusive_start_shard_id: exclusiveStartShardId)
      shards += result.stream_description.shards
      if result.stream_description.has_more_shards && shards.size > 0
        exclusiveStartShardId = shards.last.shard_id
      else
        exclusiveStartShardId = nil
      end
    end while exclusiveStartShardId
    shards
  end

  def processShard(shard, streamName)
    shard_iterator = client.get_shard_iterator(shard_id: shard.shard_id, shard_iterator_type: 'LATEST', stream_name: streamName).shard_iterator
    loop do
      result = client.get_records(shard_iterator: shard_iterator, limit: 20)
      puts("Number of records received = #{result.records.size}")
      result.records.each do |record|
        puts("Partition Key = #{record.partition_key}")
        puts("Sequence Number = #{record.sequence_number}")
        begin
          data = record.data.to_s
          puts("Data = #{data}")
          iucr_code = record.partition_key
          if (iucr_code == '1320')
            puts("ALERT!!!!!!!! IMMEDIATE ACTION REQUIRED !!!!!!")
          end
        end
      end
      sleep(1)
      shard_iterator = result.next_shard_iterator
    end
  end
end
