require "logger"
require "digest/sha1"
require "./mfile"
require "./segment_position"
require "./policy"
require "./observable"
require "./stats"
require "./sortable_json"
require "./reference_counter"
require "./queue/ready"
require "./queue/unacked"
require "./client/channel"
require "./message"

module AvalancheMQ
  class Queue
    include PolicyTarget
    include Observable
    include Stats
    include SortableJSON

    @durable = false
    @log : Logger
    @message_ttl : ArgumentNumber?
    @max_length : ArgumentNumber?
    @max_length_bytes : ArgumentNumber?
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
    @message_available = Channel(Nil).new(1)
    @consumer_available = Channel(Nil).new(1)
    @refresh_ttl_timeout = Channel(Nil).new(1)
    @ready = ReadyQueue.new
    @unacked = UnackQueue.new
    @paused = Channel(Nil).new(1)

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(%w(ack deliver get publish redeliver reject), %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers, ready, unacked
    getter policy : Policy?
    getter? closed
    property? internal = false
    getter? flow = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @last_get_time = Time.monotonic
      @log = @vhost.log.dup
      @log.progname += " queue=#{@name}"
      handle_arguments
      if @internal
        spawn expire_loop, name: "Queue#expire_loop #{@vhost.name}/#{@name}"
      else
        spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
      end
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

    # ameba:disable Metrics/CyclomaticComplexity
    def apply_policy(policy : Policy)
      clear_policy
      policy.definition.each do |k, v|
        @log.debug { "Applying policy #{k}: #{v}" }
        case k
        when "max-length"
          @max_length = v.as_i64
          drop_overflow
        when "max-length-bytes"
          @max_length_bytes = v.as_i64
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
      step_loop
    end

    # force trigger a loop in delivery_loop
    private def step_loop
      message_available
      consumer_available
    end

    def clear_policy
      handle_arguments
      return if @policy.nil?
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

    def refresh_ttl_timeout
      select
      when @refresh_ttl_timeout.send nil
      else
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

    def any? : Bool
      !empty?
    end

    def consumer_count
      @consumers.size.to_u32
    end

    private def expire_loop
      loop do
        if ttl = time_to_message_expiration
          select
          when @refresh_ttl_timeout.receive
            @log.debug "Queue#expire_loop Refresh TTL timeout"
          when timeout ttl
            expire_messages
          end
        else
          @message_available.receive
        end
      rescue Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in expire_loop: #{ex.inspect_with_backtrace}" }
      end
    end

    def flow=(@flow : Bool)
      return unless @flow
      @paused.send nil
    end

    private def deliver_loop
      i = 0
      loop do
        break if @closed
        if @ready.empty?
          i = 0
          receive_or_expire || break
        end
        if @flow && (c = find_consumer(i))
          deliver_to_consumer(c)
          # deliver 4096 msgs to a consumer then change consumer
          i = 0 if (i += 1) == 4096
        else
          break if @closed
          i = 0
          consumer_or_expire || break
        end
      rescue Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in deliver_loop: #{ex.inspect_with_backtrace}" }
      end
      @log.debug "Delivery loop closed"
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
      @log.debug { "Waiting for msgs" }
      if ttl = time_to_expiration
        select
        when @message_available.receive
        when timeout ttl
          expire_queue && return false
        end
      else
        @message_available.receive
      end
      @log.debug { "Message available" }
      true
    end

    private def consumer_or_expire
      @log.debug "No consumer available"
      q_ttl = time_to_expiration
      m_ttl = time_to_message_expiration
      ttl = {q_ttl, m_ttl}.select(Time::Span).min?
      if ttl
        @log.debug "Queue#consumer_or_expire TTL: #{ttl}"
        select
        when @paused.receive
          @log.debug { "Queue unpaused" }
        when @consumer_available.receive
          @log.debug "Queue#consumer_or_expire Consumer available"
        when @refresh_ttl_timeout.receive
          @log.debug "Queue#consumer_or_expire Refresh TTL timeout"
        when timeout ttl
          case ttl
          when q_ttl
            expire_queue && return false
          when m_ttl
            expire_messages
          else raise "Unknown TTL"
          end
        end
      elsif !@flow
        @paused.receive
      else
        @consumer_available.receive
      end
      true
    end

    private def find_consumer(i)
      # @log.debug { "Looking for available consumers" }
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
      # @log.debug { "Getting a new message" }
      if env = get(c.no_ack)
        sp = env.segment_position
        # @log.debug { "Delivering #{sp} to consumer" }
        if c.deliver(env.message, sp, env.redelivered)
          if c.no_ack
            delete_message(sp, false)
          else
            @unacked.push(sp, env.message.persistent?, c)
          end
          if env.redelivered
            @redeliver_count += 1
          else
            @deliver_count += 1
          end
          # @log.debug { "Delivery done" }
        else
          @log.debug { "Delivery failed" }
        end
      else
        @log.debug { "Consumer found, but not a message" }
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
      delete if @exclusive
      Fiber.yield
      notify_observers(:close)
      @log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      @vhost.delete_queue(@name)
      notify_observers(:delete)
      @log.debug { "Deleted" }
      true
    end

    def details_tuple
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
        messages: @ready.size + @unacked.size,
        ready: @ready.size,
        ready_bytes: @ready.sum &.bytesize,
        unacked: @unacked.size,
        unacked_bytes: @unacked.sum { |u| u.sp.bytesize },
        policy: @policy.try &.name,
        exclusive_consumer_tag: @exclusive ? @consumers.first?.try(&.tag) : nil,
        state: @closed ? :closed : :running,
        effective_policy_definition: @policy,
        message_stats: stats_details,
        internal: @internal,
      }
    end

    class RejectOverFlow < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @closed
      # @log.debug { "Enqueuing message sp=#{sp}" }
      reject_on_overflow(sp)
      drop_overflow(sp)
      was_empty = @ready.push(sp) == 1
      @publish_count += 1
      if was_empty
        message_available
      elsif sp.expiration_ts > 0
        refresh_ttl_timeout
      end
      # @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} consumers=#{@consumers.size}" }
      true
    rescue ex : RejectOverFlow
      @log.debug { "Overflow reject message sp=#{sp}" }
      raise ex
    end

    private def reject_on_overflow(sp : SegmentPosition)
      if ml = @max_length
        if @reject_on_overflow && @ready.size >= ml
          raise RejectOverFlow.new
        end
      end

      if mlb = @max_length_bytes
        if @reject_on_overflow && (@ready.sum { |r| r.bytesize } + sp.bytesize) >= mlb
          raise RejectOverFlow.new
        end
      end
    end

    private def drop_overflow(extra : SegmentPosition? = nil)
      if ml = @max_length
        extra_size = extra.nil? ? 0 : 1
        @ready.limit_size(ml - extra_size) do |sp|
          @log.debug { "Overflow drop head sp=#{sp}" }
          expire_msg(sp, :maxlen)
        end
      end

      if mlb = @max_length_bytes
        extra_size = extra.nil? ? 0 : extra.bytesize
        @ready.limit_byte_size(mlb - extra_size) do |sp|
          @log.debug { "Overflow drop head sp=#{sp}" }
          expire_msg(sp, :maxlenbytes)
        end
      end
    end

    @segment_id = 0_u32
    @segment_file = IO::Memory.new(0)

    private def segment_file(id : UInt32) : IO::Memory
      return @segment_file if @segment_id == id
      mfile = @vhost.segment_file(id)
      @segment_file = IO::Memory.new(mfile.to_slice, writeable: false)
    end

    def metadata(sp) : MessageMetadata?
      seg = segment_file(sp.segment)
      seg.seek(sp.position.to_i32, IO::Seek::Set)
      ts = Int64.from_io seg, IO::ByteFormat::SystemEndian
      ex = AMQP::ShortString.from_io seg, IO::ByteFormat::SystemEndian
      rk = AMQP::ShortString.from_io seg, IO::ByteFormat::SystemEndian
      pr = AMQP::Properties.from_io seg, IO::ByteFormat::SystemEndian
      sz = UInt64.from_io seg, IO::ByteFormat::SystemEndian
      MessageMetadata.new(ts, ex, rk, pr, sz)
    rescue ex : IO::Error
      @log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      @ready.delete sp
      delete_message sp
      nil
    end

    private def time_to_message_expiration : Time::Span?
      @log.debug { "Checking if next message has to be expired ready" }
      meta = nil
      expire_at = 0_i64
      loop do
        sp = @ready.first? || return
        expire_at = sp.expiration_ts || 0_i64
        break if expire_at > 0
        if message_ttl = @message_ttl
          if meta = metadata(sp)
            expire_at = meta.timestamp + message_ttl
            break
          end
        else
          return
        end
      end
      expire_in = expire_at - RoughTime.utc.to_unix_ms
      if expire_in > 0
        expire_in.milliseconds
      else
        Time::Span.zero
      end
    end

    private def expire_messages : Nil
      i = 0
      now = RoughTime.utc.to_unix_ms
      @ready.shift do |sp|
        @log.debug { "Checking if next message has to be expired" }
        expire_at = sp.expiration_ts
        env = nil
        if expire_at.zero?
          if @message_ttl.nil?
            @log.debug { "No more message to expire" }
            next false
          end
          env = read(sp)
          expire_at = env.message.timestamp + (@message_ttl || 0)
        end
        expire_in = expire_at - now
        if expire_in <= 0
          env ||= read(sp)
          expire_msg(env, :expired)
          if (i += 1) == 8192
            Fiber.yield
            i = 0
          end
          true
        else
          @log.debug { "No more message to expire" }
          false
        end
      end
      @log.info { "Expired #{i} messages" } if i > 0
    end

    private def expire_msg(sp : SegmentPosition, reason : Symbol)
      env = read(sp)
      expire_msg(env, reason)
    end

    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      dlx = msg.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
      if dlx
        dlrk = msg.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || msg.routing_key
        props = handle_dlx_header(msg, reason)
        dead_letter_msg(msg, sp, props, dlx, dlrk)
      end
      delete_message sp, msg.persistent?
    rescue ex : IO::EOFError
      raise ex
    end

    private def handle_dlx_header(meta, reason)
      props = meta.properties.clone
      headers = props.headers || AMQP::Table.new
      headers.delete("x-dead-letter-exchange")
      headers.delete("x-dead-letter-routing-key")
      headers.delete("x-delay")

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
        "time"         => RoughTime.utc,
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

    private def dead_letter_msg(msg : BytesMessage, sp, props, dlx, dlrk)
      @log.debug { "Dead lettering #{sp}, ex=#{dlx} rk=#{dlrk} body_size=#{msg.size} props=#{props}" }
      @vhost.publish Message.new(msg.timestamp, dlx.to_s, dlrk.to_s,
        props, msg.size, IO::Memory.new(msg.body))
    end

    private def expire_queue(now = Time.monotonic) : Bool
      return false unless @consumers.empty?
      @log.debug "Expired"
      @vhost.delete_queue(@name)
      true
    end

    def basic_get(no_ack) : Envelope?
      return nil unless @flow
      @last_get_time = Time.monotonic
      @get_count += 1
      if env = get(no_ack)
        if no_ack
          delete_message(env.segment_position, false)
        else
          @unacked.push(env.segment_position, env.message.persistent?, nil)
        end
        env
      end
    end

    # return the next message in the ready queue
    private def get(no_ack : Bool)
      return nil if @closed
      if sp = @ready.shift?
        env = read(sp)
        if @delivery_limit && !no_ack
          with_delivery_count_header(env)
        else
          env
        end
      end
    end

    private def with_delivery_count_header(env)
      if limit = @delivery_limit
        sp = env.segment_position
        headers = env.message.properties.headers || AMQP::Table.new
        delivery_count = @deliveries.fetch(sp, 0)
        # @log.debug { "Delivery count: #{delivery_count} Delivery limit: #{@delivery_limit}" }
        if delivery_count >= limit
          expire_msg(env, :delivery_limit)
          return nil
        end
        headers["x-delivery-count"] = @deliveries[sp] = delivery_count + 1
        env.message.properties.headers = headers
      end
      env
    end

    def read(sp : SegmentPosition)
      seg = segment_file(sp.segment)
      seg.seek(sp.position.to_i32, IO::Seek::Set)
      ts = Int64.from_io seg, IO::ByteFormat::SystemEndian
      ex = AMQP::ShortString.from_io seg, IO::ByteFormat::SystemEndian
      rk = AMQP::ShortString.from_io seg, IO::ByteFormat::SystemEndian
      pr = AMQP::Properties.from_io seg, IO::ByteFormat::SystemEndian
      sz = UInt64.from_io seg, IO::ByteFormat::SystemEndian
      body = seg.to_slice[seg.pos, sz]
      msg = BytesMessage.new(ts, ex, rk, pr, sz, body)
      redelivered = @requeued.includes?(sp)
      Envelope.new(sp, msg, redelivered)
    rescue ex : IO::Error
      @log.error { "Message #{sp} not found, possible message loss. #{ex.inspect}" }
      @ready.delete(sp)
      delete_message sp
      raise ex
    rescue ex
      @log.error "Error reading message at #{sp}: #{ex.inspect_with_backtrace}"
      raise ex
    ensure
      @requeued.delete(sp) if redelivered
    end

    def ack(sp : SegmentPosition, persistent : Bool) : Nil
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @ack_count += 1
      @unacked.delete(sp)
      delete_message(sp, persistent)
      consumer_available
    end

    protected def delete_message(sp : SegmentPosition, persistent = false) : Nil
      @deliveries.delete(sp) if @delivery_limit
      if @ready.capacity > 10_000 && @ready.capacity > @ready.size * 2
        elapsed = Time.measure do
          @ready.compact
        end
        @log.info { "Compacting ready queue took #{elapsed.total_milliseconds} ms" }
      end
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted
      @log.debug { "Rejecting #{sp}, requeue: #{requeue}" }
      @unacked.delete(sp)
      if requeue
        was_empty = @ready.insert(sp) == 1
        @requeued << sp
        message_available if was_empty
      else
        expire_msg(sp, :rejected)
      end
      @reject_count += 1
    end

    private def requeue_many(sps : Enumerable(SegmentPosition))
      return if @deleted
      return if sps.empty?
      @log.debug { "Returning #{sps.size} msgs to ready state" }
      @reject_count += sps.size
      was_empty = @ready.insert(sps) == sps.size
      message_available if was_empty
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers_lock.synchronize do
        @consumers.push consumer
      end
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      consumer_available
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer, basic_cancel = false)
      deleted = @consumers_lock.synchronize { @consumers.delete consumer }
      if deleted
        @exclusive_consumer = false if consumer.exclusive
        consumer_unacked_size = @unacked.sum { |u| u.consumer == consumer ? 1 : 0 }
        unless basic_cancel
          requeue_many(@unacked.delete(consumer))
        end

        @log.debug { "Removing consumer with #{consumer_unacked_size} \
                      unacked messages \
                      (#{@consumers.size} consumers left)" }
        notify_observers(:rm_consumer, consumer)
        delete if @consumers.empty? && @auto_delete
      end
    end

    def purge : UInt32
      count = @ready.purge
      @log.debug { "Purged #{count} messages" }
      count.to_u32
    end

    def match?(frame)
      @durable == frame.durable &&
        @exclusive == frame.exclusive &&
        @auto_delete == frame.auto_delete &&
        @arguments == frame.arguments.to_h
    end

    def match?(durable, exclusive, auto_delete, arguments)
      @durable == durable &&
        @exclusive == exclusive &&
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

    class Error < Exception; end
  end
end
