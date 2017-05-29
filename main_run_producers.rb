# frozen_string_literal: true

require_relative 'aws_chicago_crimes_producers'
require_relative 'manage_kinesis_streams'

stream_name = 'EitoballTestStreamingService'
streams = ManageKinesisStreams.new
streams.createKinesisStream(stream_name, 1)
producers = AwsChicagoCrimesProducers.new
producers.readAndSubmitBatch(stream_name, 10)
