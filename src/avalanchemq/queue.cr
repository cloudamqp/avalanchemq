require "digest/sha1"
require "./segment_position"
require "./policy"
require "./observable"
require "./stats"
require "./sortable_json"
require "./client/channel"

module AvalancheMQ
  class Queue
    include PolicyTarget
    include Observable
    include Stats
    include SortableJSON

    alias ArgumentNumber = UInt16 | Int32 | Int64

    @durable = false
    @message_ttl : ArgumentNumber?
    @max_length : ArgumentNumber?
    @expires : ArgumentNumber?
    @delivery_limit : ArgumentNumber?
    @dlx : String?
    @dlrk : String?
    @reject_on_overflow = false
    @closed = false
    @deleted = false
    @exclusive_consumer = false
    @requeued = Set(SegmentPosition).new
    @deliveries = Hash(SegmentPosition, Int32).new
    @read_lock = Mutex.new(:reentrant)
    @consumers = Deque(Client::Channel::Consumer).new
    @consumers_lock = Mutex.new(:unchecked)
    @message_available = Channel(Nil).new
    @consumer_available = Channel(Nil).new(1)
    @ready = Deque(SegmentPosition).new(1024)
    @ready_lock = Mutex.new(:reentrant)
    @segment_pos = Hash(UInt32, UInt32).new { 0_u32 }
    @unacked = Deque(Unack).new(1024)
    @unack_lock = Mutex.new(:unchecked)

    record Unack,
      sp : SegmentPosition,
      persistent : Bool,
      consumer : Client::Channel::Consumer?

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(%w(ack deliver get publish redeliver reject), %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers
    getter policy : Policy?
    getter? closed

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @last_get_time = Time.monotonic
      handle_arguments
      spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << " @vhost=" << @vhost.name << ">"
    end

    def self.generate_name
      "amq.gen-#{Random::Secure.urlsafe_base64(24)}"
    end

    def redeclare
      @last_get_time = Time.monotonic
    end

    def has_exclusive_consumer?
      @exclusive_consumer
    end

    def apply_policy(policy : Policy)
      clear_policy
      policy.definition.each do |k, v|
        Log.debug { "Applying policy #{k}: #{v}" }
        case k
        when "max-length"
          @max_length = v.as_i64
          drop_overflow
        when "message-ttl"
          @message_ttl = v.as_i64
          expire_messages
        when "overflow"
          @reject_on_overflow = v.as_s == "reject-publish"
        when "expires"
          @expires = v.as_i64
        when "dead-letter-exchange"
          @dlx = v.as_s
        when "dead-letter-routing-key"
          @dlrk = v.as_s
        when "federation-upstream"
          @vhost.upstreams.try &.link(v.as_s, self)
        when "federation-upstream-set"
          @vhost.upstreams.try &.link_set(v.as_s, self)
        when "delivery-limit"
          @delivery_limit = v.as_i64
        else nil
        end
      end
      @policy = policy
      # force trigger a loop in delivery_loop
      message_available
      consumer_available
    end

    def clear_policy
      Log.debug { "Clearing policy" }
      handle_arguments
      @policy = nil
      @vhost.upstreams.try &.stop_link(self)
    end

    def unacked_count
      @unacked.size.to_u32
    end

    def message_available
      select
      when @message_available.send nil
      else
      end
    end

    def consumer_available
      select
      when @consumer_available.send nil
      else
      end
    end

    private def drop_overflow
      if ml = @max_length
        @ready_lock.synchronize do
          while @ready.size > ml
            drophead || break
          end
        end
      end
    end

    private def handle_arguments
      @message_ttl = @arguments["x-message-ttl"]?.try &.as?(ArgumentNumber)
      @expires = @arguments["x-expires"]?.try &.as?(ArgumentNumber)
      @dlx = @arguments["x-dead-letter-exchange"]?.try &.to_s
      @dlrk = @arguments["x-dead-letter-routing-key"]?.try &.to_s
      @max_length = @arguments["x-max-length"]?.try &.as?(ArgumentNumber)
      @delivery_limit = @arguments["x-delivery-limit"]?.try &.as?(ArgumentNumber)
      @reject_on_overflow = @arguments.fetch("x-overflow", "").to_s == "reject-publish"
    end

    def immediate_delivery?
      @consumers_lock.synchronize do
        @consumers.any? { |c| c.accepts? }
      end
    end

    def message_count
      @ready.size.to_u32
    end

    def empty? : Bool
      @ready.size.zero?
    end

    def consumer_count
      @consumers.size.to_u32
    end


    def referenced_segments(s : Set(UInt32))
      @unack_lock.synchronize do
        @unacked.each do |u|
          s << u.sp.segment
        end
      end
      @ready_lock.synchronize do
        @ready.each do |sp|
          s << sp.segment
        end
      end
    end

    def close_segments
      @read_lock.synchronize do
        @segment_file.try &.close
      end
    end

    private def deliver_loop
      i = 0
      loop do
        break if @closed
        if @ready.empty?
          i = 0
          receive_or_expire || break
        end
        if c = find_consumer(i)
          deliver_to_consumer(c)
        else
          break if @closed
          i = 0
          consumer_or_expire || break
        end
        if i >= 1024
          i = 0
        end
      rescue Channel::ClosedError
        break
      rescue ex
        Log.error { "Unexpected exception in deliver_loop: #{ex.inspect_with_backtrace}" }
      end
      Log.debug { "Delivery loop closed" }
    end

    private def time_to_expiration : Time::Span?
      if e = @expires
        expires_in = @last_get_time + e.milliseconds - Time.monotonic
        if expires_in > Time::Span.zero
          expires_in
        else
          Time::Span.zero
        end
      end
    end

    private def receive_or_expire
      Log.debug { "Waiting for msgs" }
      if ttl = time_to_expiration
        select
        when @message_available.receive
        when timeout ttl
          expire_queue && return false
        end
      else
        @message_available.receive
      end
      Log.debug { "Message available" }
      true
    end

    private def consumer_or_expire
      Log.debug { "No consumer available" }
      q_ttl = time_to_expiration
      m_ttl = time_to_message_expiration
      ttl = { q_ttl, m_ttl }.select(Time::Span).min?
      if ttl
        select
        when @consumer_available.receive
          Log.debug { "Consumer available" }
        when timeout ttl
          case ttl
          when q_ttl
            expire_queue && return false
          when m_ttl
            expire_messages
          else raise "Unknown TTL"
          end
        end
      else
        @consumer_available.receive
      end
      true
    end

    private def find_consumer(i)
      #Log.debug { "Looking for available consumers" }
      case @consumers.size
      when 0
        nil
      when 1
        c = @consumers[0]
        c.accepts? ? c : nil
      else
        if i > 0 # reuse same consumer for a while if we're delivering fast
          c = @consumers[0]
          return c if c.accepts?
        end
        @consumers_lock.synchronize do
          @consumers.size.times do
            c = @consumers.shift
            @consumers.push c
            return c if c.accepts?
          end
        end
        nil
      end
    end

    private def deliver_to_consumer(c)
      #Log.debug { "Getting a new message" }
      get(c.no_ack) do |env|
        if env
          sp = env.segment_position
          #Log.debug { "Delivering #{sp} to consumer" }
          if c.deliver(env.message, sp, env.redelivered)
            unless c.no_ack
              @unack_lock.synchronize do
                @unacked << Unack.new(sp, env.message.persistent?, c)
              end
            end
            if env.redelivered
              @redeliver_count += 1
            else
              @deliver_count += 1
            end
            #Log.debug { "Delivery done" }
          else
            Log.debug { "Delivery failed" }
          end
        else
          Log.debug { "Consumer found, but not a message" }
        end
      end
    end

    def close : Bool
      return false if @closed
      @closed = true
      @message_available.close
      @consumer_available.close
      @consumers_lock.synchronize do
        @consumers.each &.cancel
        @consumers.clear
      end
      @segment_file.try &.close
      delete if @exclusive
      Fiber.yield
      notify_observers(:close)
      Log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      @vhost.delete_queue(@name)
      notify_observers(:delete)
      Log.debug { "Deleted" }
      true
    end

    def details_tuple
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
        messages: @ready.size + @unacked.size,
        ready: @ready.size,
        unacked: @unacked.size,
        policy: @policy.try &.name,
        exclusive_consumer_tag: @exclusive ? @consumers.first?.try(&.tag) : nil,
        state: @closed ? :closed : :running,
        effective_policy_definition: @policy,
        message_stats: stats_details,
      }
    end

    class RejectOverFlow < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @closed
      #Log.debug { "Enqueuing message sp=#{sp}" }
      reject_on_overflow
      was_empty = false
      @ready_lock.synchronize do
        drop_on_overflow
        was_empty = @ready.empty?
        @ready.push sp
      end
      @publish_count += 1
      message_available if was_empty
      #Log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} consumers=#{@consumers.size}" }
      true
    rescue ex : RejectOverFlow
      Log.debug { "Overflow reject message sp=#{sp}" }
      raise ex
    end

    private def reject_on_overflow
      if ml = @max_length
        if @reject_on_overflow && @ready.size >= ml
          raise RejectOverFlow.new
        end
      end
    end

    private def drop_on_overflow
      if ml = @max_length
        while @ready.size >= ml
          drophead || break
        end
      end
    end

    @segment_file : File? = nil
    @segment_id = 0_u32
    @segment_pos = 0_u32

    private def segment_file(id : UInt32) : File
      return @segment_file.not_nil! if @segment_id == id && @segment_file
      path = File.join(@vhost.data_dir, "msgs.#{id.to_s.rjust(10, '0')}")
      @segment_file.try &.close
      @segment_id = id
      @segment_pos = 0
      @segment_file = File.open(path, "r").tap do |f|
        f.buffer_size = Config.instance.file_buffer_size
      end
    end

    private def metadata(sp) : MessageMetadata?
      @read_lock.synchronize do
        seg = segment_file(sp.segment)
        if @segment_pos != sp.position
          Log.debug { "Seeking to #{sp.position}, was at #{@segment_pos}" }
          seg.seek(sp.position, IO::Seek::Set)
          @segment_pos = sp.position
        end
        ts = Int64.from_io seg, IO::ByteFormat::NetworkEndian
        ex = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
        rk = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
        pr = AMQP::Properties.from_io seg, IO::ByteFormat::NetworkEndian
        sz = UInt64.from_io seg, IO::ByteFormat::NetworkEndian
        meta = MessageMetadata.new(ts, ex, rk, pr, sz)
        @segment_pos = sp.position + meta.bytesize
        meta
      end
    rescue ex : IO::Error
      Log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      drop(sp, true, true)
      nil
    rescue ex : IO::EOFError
      pos = segment_file(sp.segment).pos.to_u32
      Log.error(exception: ex) { "EOF when reading metadata for sp=#{sp}, is at=#{pos}" }
      @segment_pos = pos
      drop(sp, true, true)
      nil
    end

    private def time_to_message_expiration : Time::Span?
      Log.debug { "Checking if next message has to be expired" }
      meta = nil
      until meta
        sp = @ready_lock.synchronize { @ready[0]? } || return
        meta = metadata(sp)
      end
      Log.debug { "Next message: #{meta}" }
      exp_ms = meta.properties.expiration.try(&.to_i64?) || @message_ttl
      if exp_ms
        expire_at = meta.timestamp + exp_ms
        expire_in = expire_at - RoughTime.utc.to_unix_ms
        if expire_in > 0
          expire_in.milliseconds
        else
          Time::Span.zero
        end
      end
    end

    private def expire_messages : Nil
      @ready_lock.lock
      @read_lock.lock
      i = 0
      now = RoughTime.utc.to_unix_ms
      loop do
        sp = @ready[0]? || break
        Log.debug { "Checking if next message has to be expired" }
        read(sp) do |env|
          Log.debug { "Next message: #{env.message}" }
          exp_ms = env.message.properties.expiration.try(&.to_i64?) || @message_ttl
          if exp_ms
            expire_at = env.message.timestamp + exp_ms
            expire_in = expire_at - now
            if expire_in <= 0
              @ready.shift
              expire_msg(env, :expired)
              if (i += 1) == 8192
                Fiber.yield
                i = 0
              end
            else
              Log.debug { "No more message to expire" }
              return
            end
          else
            Log.debug { "No more message to expire" }
            return
          end
        end
      end
      Log.info { "Expired #{i} messages" } if i > 0
    ensure
      @read_lock.unlock
      @ready_lock.unlock
    end

    private def expire_msg(sp : SegmentPosition, reason : Symbol)
      read(sp) do |env|
        expire_msg(env, reason)
      end
    end

    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      Log.debug { "Expiring #{sp} now due to #{reason}" }
      dlx = msg.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
      if dlx
        dlrk = msg.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || msg.routing_key
        props = handle_dlx_header(msg, reason)
        dead_letter_msg(msg, sp, props, dlx, dlrk)
      else
        msg.body_io.skip(msg.size)
      end
      persistent = msg.properties.delivery_mode == 2_u8
      drop sp, false, persistent
    rescue ex : IO::EOFError
      @segment_pos = segment_file(env.segment_position.segment).pos.to_u32
      raise ex
    end

    private def handle_dlx_header(meta, reason)
      props = meta.properties.clone
      headers = props.headers || AMQP::Table.new
      headers.delete("x-dead-letter-exchange")
      headers.delete("x-dead-letter-routing-key")

      xdeaths = Array(AMQP::Table).new(1)
      if headers.has_key? "x-death"
        headers["x-death"].as?(Array(AMQP::Field)).try &.each do |tbl|
          xdeaths << tbl.as(AMQP::Table)
        end
      end
      xd = xdeaths.find { |d| d["queue"] == @name && d["reason"] == reason.to_s }
      xdeaths.delete(xd)
      count = xd.try &.fetch("count", 0).as?(Int32) || 0
      death = Hash(String, AMQP::Field){
        "exchange"     => meta.exchange_name,
        "queue"        => @name,
        "routing-keys" => [meta.routing_key.as(AMQP::Field)],
        "reason"       => reason.to_s,
        "count"        => count + 1,
        "time"         => Time.utc,
      }
      if props.expiration
        death["original-expiration"] = props.expiration
        props.expiration = nil
      end
      xdeaths.unshift AMQP::Table.new(death)

      headers["x-death"] = xdeaths
      props.headers = headers
      props
    end

    private def dead_letter_msg(msg : Message, sp, props, dlx, dlrk)
      #Log.debug { "Dead lettering #{sp}, ex=#{dlx} rk=#{dlrk} body_size=#{msg.size} props=#{props}" }
      ok = @vhost.publish Message.new(msg.timestamp, dlx.to_s, dlrk.to_s,
                                      props, msg.size, msg.body_io)
      msg.body_io.skip(msg.size) if ok.nil?
    end

    private def expire_queue(now = Time.monotonic) : Bool
      return false unless @consumers.empty?
      Log.debug { "Expired" }
      @vhost.delete_queue(@name)
      true
    end

    def basic_get(no_ack, &blk : Envelope? -> Nil)
      @last_get_time = Time.monotonic
      get(no_ack) do |env|
        if env
          @get_count += 1
          unless no_ack
            @unack_lock.synchronize do
              @unacked << Unack.new(env.segment_position,
                                    env.message.persistent?,
                                    nil)
            end
          end
        end
        yield env
      end
    end

    private def get(no_ack : Bool, &blk : Envelope? -> Nil)
      return yield nil if @closed
      sp = @ready_lock.synchronize { @ready.shift? }
      return yield nil if sp.nil?
      read(sp) do |env|
        if @delivery_limit && !no_ack
          yield with_delivery_count_header(env)
        else
          yield env
        end
      end
    end

    private def with_delivery_count_header(env)
      if limit = @delivery_limit
        sp = env.segment_position
        headers = env.message.properties.headers || AMQP::Table.new
        delivery_count = @deliveries.fetch(sp, 0)
        #Log.debug { "Delivery count: #{delivery_count} Delivery limit: #{@delivery_limit}" }
        if delivery_count >= limit
          @ready_lock.synchronize do
            expire_msg(env, :delivery_limit)
          end
          return nil
        end
        headers["x-delivery-count"] = @deliveries[sp] = delivery_count + 1
        env.message.properties.headers = headers
      end
      env
    end

    def read(sp : SegmentPosition, &blk : Envelope -> Nil)
      @read_lock.lock
      seg = segment_file(sp.segment)
      if @segment_pos != sp.position
        Log.debug { "Seeking to #{sp.position}, was at #{@segment_pos}" }
        seg.seek(sp.position, IO::Seek::Set)
      end
      ts = Int64.from_io seg, IO::ByteFormat::NetworkEndian
      ex = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
      rk = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
      pr = AMQP::Properties.from_io seg, IO::ByteFormat::NetworkEndian
      sz = UInt64.from_io seg, IO::ByteFormat::NetworkEndian
      msg = Message.new(ts, ex, rk, pr, sz, seg)
      redelivered = @requeued.includes?(sp)
      begin
        yield Envelope.new(sp, msg, redelivered)
      ensure
        @segment_pos = sp.position + msg.bytesize
        @requeued.delete(sp) if redelivered
      end
    rescue ex : IO::Error
      Log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      drop sp, true, true
    rescue ex
      if seg
        Log.error { "Error reading message at #{sp}: #{ex.inspect}" }
        Log.error { "Hexdump of the first 1024 bytes on disk:" }
        seg.seek(sp.position, IO::Seek::Set)
        io = IO::Hexdump.new(seg, output: STDERR, read: true)
        io.skip 1024
      end
      @segment_pos = segment_file(sp.segment).pos.to_u32
      raise ex
    ensure
      @read_lock.unlock
    end

    def ack(sp : SegmentPosition, persistent : Bool) : Nil
      return if @deleted
      Log.debug { "Acking #{sp}" }
      @unack_lock.synchronize do
        if idx = @unacked.index { |u| u.sp == sp }
          @unacked.delete_at(idx)
        end
      end
      @deliveries.delete(sp)
      @ack_count += 1
      consumer_available
    end

    # dropping a specific message/segmentposition
    private def drop(sp, in_ready, persistent) : Nil
      return if @deleted
      Log.debug { "Dropping #{sp}" }

      if in_ready
        @ready_lock.synchronize do
          if @ready.empty?
            Log.debug { "Dropping #{sp} but ready queue is empty" }
          elsif @ready.first == sp
            @ready.shift
          else
            Log.debug { "Dropping #{sp} wasn't at the head of the ready queue" }
            if idx = @ready.bsearch_index { |rsp| rsp >= sp }
              if @ready[idx] == sp
                @ready.delete_at(idx)
              else
                Log.error { "Dropping #{sp} but wasn't in ready queue" }
              end
            else
              Log.error { "Dropping #{sp} but wasn't in ready queue" }
            end
          end
        end
      end
      @deliveries.delete(sp)
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted
      Log.debug { "Rejecting #{sp}" }

      @unack_lock.synchronize do
        if idx = @unacked.index { |u| u.sp == sp }
          @unacked.delete_at(idx)
        end
      end
      @ready_lock.synchronize do
        if requeue
          was_empty = @ready.empty?
          i = @ready.bsearch_index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @requeued << sp
          message_available if was_empty
        else
          expire_msg(sp, :rejected)
        end
      end
      @reject_count += 1
    end

    private def requeue_many(sps : Enumerable(SegmentPosition))
      return if @deleted
      return if sps.empty?
      was_empty = false
      Log.debug { "Returning #{sps.size} msgs to ready state" }
      @reject_count += sps.size
      @ready_lock.synchronize do
        was_empty = @ready.empty?
        sps.reverse_each do |sp|
          i = @ready.bsearch_index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @requeued << sp
        end
      end
      message_available if was_empty
    end

    private def drophead : Bool
      if sp = @ready.shift?
        Log.debug { "Overflow drop head sp=#{sp}" }
        expire_msg(sp, :maxlen)
        true
      else
        false
      end
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers_lock.synchronize do
        @consumers.push consumer
      end
      @exclusive_consumer = true if consumer.exclusive
      Log.debug { "Adding consumer (now #{@consumers.size})" }
      consumer_available
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      deleted = @consumers_lock.synchronize { @consumers.delete consumer }
      if deleted
        @exclusive_consumer = false if consumer.exclusive
        consumer_unacked = Array(SegmentPosition).new(consumer.prefetch_count)
        @unacked.delete_if do |unack|
          if unack.consumer == consumer
            consumer_unacked << unack.sp
            true
          end
        end
        requeue_many(consumer_unacked)
        Log.debug { "Removing consumer with #{consumer_unacked.size} \
                      unacked messages \
                      (#{@consumers.size} consumers left)" }
        notify_observers(:rm_consumer, consumer)
        delete if @consumers.empty? && @auto_delete
      end
    end

    def purge : UInt32
      @ready_lock.synchronize do
        purged_count = @ready.size
        @ready.clear
        Log.debug { "Purged #{purged_count} messages" }
        purged_count.to_u32
      end
    end

    def match?(frame)
      @durable == frame.durable &&
        @exclusive == frame.exclusive &&
        @auto_delete == frame.auto_delete &&
        @arguments == frame.arguments.to_h
    end

    def match?(durable, auto_delete, arguments)
      @durable == durable &&
        @auto_delete == auto_delete &&
        @arguments == arguments.to_h
    end

    def in_use?
      !(empty? && @consumers.empty?)
    end

    def fsync_enq
    end

    def fsync_ack
    end
  end
end
