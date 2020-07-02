require "./spec_helper"

describe AvalancheMQ::PriorityQueue do
  it "should prioritize messages" do
    with_channel do |ch|
      q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
      q = ch.queue("", args: q_args)
      q.publish "prio2", props: AMQP::Client::Properties.new(priority: 2)
      q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
      q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("prio2")
      q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("prio1")
    end
  end

  it "should prioritize messages as 0 if no prio is set" do
    with_channel do |ch|
      q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
      q = ch.queue("", args: q_args)
      q.publish "prio0"
      q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
      q.publish "prio00"
      q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("prio1")
      q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("prio0")
      q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("prio00")
    end
  end
end
