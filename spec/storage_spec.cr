require "./spec_helper"

describe AvalancheMQ::DurableQueue do
  it "GC message index after MAX_ACKS" do
    sp_size = AvalancheMQ::SegmentPosition::BYTESIZE
    raw_sp_size = AvalancheMQ::RawSegmentPosition::BYTESIZE
    max_acks = AvalancheMQ::Config.instance.queue_max_acks
    with_channel do |ch|
      q = ch.queue("d", durable: true)
      queue = s.vhosts["/"].queues["d"].as(AvalancheMQ::DurableQueue)
      queue.enq_file_size.should eq sizeof(Int32)
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      2.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq((max_acks + 2) * sp_size + sizeof(Int32))
      acks = 0
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        acks += 1
        case acks
        when max_acks - 1
          sleep 0.1
          queue.ack_file_size.should eq (max_acks - 1) * raw_sp_size + sizeof(Int32)
        when max_acks
          sleep 0.2
          queue.enq_file_size.should eq 2 * sp_size + sizeof(Int32)
          queue.ack_file_size.should eq 0 * raw_sp_size + sizeof(Int32)
          q.unsubscribe("tag")
        end
      end
    ensure
      ch.queue_delete "d"
    end
  end

  pending "GC message index when msgs are dead-lettered" do
    sp_size = AvalancheMQ::SegmentPosition::BYTESIZE
    max_acks = AvalancheMQ::Config.instance.queue_max_acks
    raw_sp_size = AvalancheMQ::RawSegmentPosition::BYTESIZE
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      q = ch.queue("ml", durable: true, args: args)
      queue = s.vhosts["/"].queues["ml"].as(AvalancheMQ::DurableQueue)
      queue.enq_file_size.should eq sizeof(Int32)
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      1.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq(1 * sp_size + sizeof(Int32))
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        sleep 0.2
        queue.ack_file_size.should eq 1 * raw_sp_size + sizeof(Int32)
        queue.enq_file_size.should eq 1 * sp_size + sizeof(Int32)
        q.unsubscribe("tag")
      end
    end
  end
end

describe AvalancheMQ::VHost do
  it "should not be dirty if nothing happend" do
    s.vhosts.create("not_dirty").dirty?.should be_false
  end

  it "should be dirty after ack" do
    with_channel do |ch|
      q = ch.queue
      q.publish "msg"
      done = false
      q.subscribe(no_ack: true) do |_msg|
        done = true
      end
      wait_for { done }
      s.vhosts["/"].dirty?.should be_true
    end
  end

  it "should be dirty after reject" do
    with_channel do |ch|
      q = ch.queue
      q.publish "msg"
      done = false
      q.subscribe(no_ack: false) do |msg|
        msg.reject
        done = true
      end
      wait_for { done }
      s.vhosts["/"].dirty?.should be_true
    end
  end

  it "should be dirty after expire" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "expired", props: AMQP::Client::Properties.new(expiration: "0")
      s.vhosts["/"].dirty?.should be_true
    end
  end

  pending "GC segments" do
    vhost = s.vhosts["/"]
    vhost.queues.each_value &.delete
    vhost.queues.clear

    msg_size = 5120
    overhead = 21
    body = Bytes.new(msg_size)

    segments = ->{ Dir.new(vhost.data_dir).children.select!(/^msgs\./) }
    sleep AvalancheMQ::Config.instance.gc_segments_interval + 0.2

    size_of_current_segment = File.size(File.join(vhost.data_dir, segments.call.last))

    msgs_to_fill_2_segments = ((AvalancheMQ::Config.instance.segment_size * 2 - size_of_current_segment) / (msg_size + overhead)).ceil.to_i

    with_channel do |ch|
      ch.confirm_select
      msgid = 0_u64
      q = ch.queue("dd", durable: true)
      msgs_to_fill_2_segments.times do
        msgid = q.publish body
      end
      ch.wait_for_confirm(msgid)
      segments.call.size.should eq 2
      q.purge
      sleep AvalancheMQ::Config.instance.gc_segments_interval + 0.4
      segments.call.size.should eq 1
    end
  end
end
