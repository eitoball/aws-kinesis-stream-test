# frozen_string_literal: true

require_relative 'aws_chicago_crimes_consumers'
require_relative 'manage_kinesis_streams'

stream_name = 'EitoballTestStreamingService'
consumers = AwsChicagoCrimesConsumers.new
consumers.consumeAndAlert(stream_name)
