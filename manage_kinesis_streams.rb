# frozen_string_literal: true

require 'bundler/setup'

require 'aws-sdk'

class ManageKinesisStreams
  def initialize
    credentials = Aws::SharedCredentials.new
    credentials.credentials.tap do |c|
      puts("Access Key = #{c.access_key_id}")
      puts("Secret Key = #{c.secret_access_key}")
    end
    @client = Aws::Kinesis::Client.new(
      region: 'ap-northeast-1', credentials: credentials)
  end

  def createKinesisStream(streamName, shards)
    puts("Creating new Stream = '#{streamName}' with Shards = #{shards}")
    unless checkStreamExists(streamName)
      begin
        Timeout.timeout(30_000) do
          client.create_stream(stream_name: streamName, shard_count: shards)
        end
      rescue TimeoutError
      end
    end
  end

  def deleteKinesisStream(streamName)
    if checkStreamExists(streamName)
      client.delete_stream(stream_name: streamName)
      puts("Deleted the Kinesis Stream = '#{streamName}'")
    else
      puts("Stream does not exists = #{streamName}")
    end
  end

  def checkStreamExists(streamName)
    result = client.describe_stream(stream_name: streamName)
    puts("Kinesis Stream '#{streamName}' already exists...")
    puts("Status of '#{streamName}' = #{result.stream_description.stream_status}")
    true
  rescue Aws::Kinesis::Errors::ResourceNotFoundException
    false
  end

  private

  attr_reader :client
end
