# frozen_string_literal: true

require 'aws-sdk'
require 'csv'

class AwsChicagoCrimesProducers
  def initialize
    @client = Aws::Kinesis::Client.new(region: 'ap-northeast-1')
  end

  def readSingleRecordAndSubmit(streamName)
    CSV.foreach('ChicagoCrimes-Aug-2015.csv') do |row|
      iucr_code = row[4]
      puts("Submitting Record = #{row.to_csv}")
      client.put_records(records: [{ data: row.to_csv, partition_key: iucr_code }], stream_name: streamName)
      sleep(0.5)
    end
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  def readAndSubmitBatch(streamName, batchSize)
    csv = CSV.open('ChicagoCrimes-Aug-2015.csv')
    csv.each.each_slice(batchSize) do |rows|
      data = rows.each_with_object([]) do |row, arr|
        arr << { data: row.to_csv, partition_key: row[4] }
      end
      puts("Submitting Records = #{data.size}")
      client.put_records(records: data, stream_name: streamName)
      sleep(0.5)
    end
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  private

  attr_reader :client
end
