require "logger"
require "digest/sha1"
require "../mfile"
require "../segment_position"
require "../policy"
require "../observable"
require "../stats"
require "../sortable_json"
require "./ready"
require "./unacked"
require "../client/channel"
require "../message"
require "../error"
require "../consumer_store"

module AvalancheMQ
  enum QueueState
    Running
    Paused
    Flow
    Closed
    Deleted

    def to_s
      super.downcase
    end
  end

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
    @exclusive_consumer = false
    @requeued = Set(SegmentPosition).new
    @deliveries = Hash(SegmentPosition, Int32).new
    @read_lock = Mutex.new(:reentrant)
    @consumers = ConsumerStore.new
    @message_available = Channel(Nil).new(1)
    @consumer_available = Channel(Nil).new(1)
    @refresh_ttl_timeout = Channel(Nil).new(1)
    @unacked = UnackQueue.new
    @paused = Channel(Nil).new(1)

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(
      %w(ack deliver confirm get get_no_ack publish redeliver reject return_unroutable),
      %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers, ready,
      unacked, last_get_time
    getter policy : Policy?
    getter? closed
    property? internal = false
    getter state = QueueState::Running

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new,
                   @ready = ReadyQueue.new)
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

    def bindings
      @vhost.queue_bindings(self)
    end

    def redeclare
      @last_get_time = Time.monotonic
      step_loop # necessary to recalculate ttl
    end

    def has_exclusive_consumer?
      @exclusive_consumer
    end

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
          @last_get_time = Time.monotonic
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
      @dlx = parse_header("x-dead-letter-exchange", String)
      @dlrk = parse_header("x-dead-letter-routing-key", String)
      if @dlrk && @dlx.nil?
        raise AvalancheMQ::Error::PreconditionFailed.new("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
      end
      @expires = parse_header("x-expires", ArgumentNumber)
      validate_gt_zero("x-expires", @expires)
      @max_length = parse_header("x-max-length", ArgumentNumber)
      validate_positive("x-max-length", @max_length)
      @max_length_bytes = parse_header("x-max-length-bytes", ArgumentNumber)
      validate_positive("x-max-length-bytes", @max_length_bytes)
      @message_ttl = parse_header("x-message-ttl", ArgumentNumber)
      validate_positive("x-message-ttl", @message_ttl)
      @delivery_limit = parse_header("x-delivery-limit", ArgumentNumber)
      validate_positive("x-delivery-limit", @delivery_limit)
      @reject_on_overflow = parse_header("x-overflow", String) == "reject-publish"
    end

    private macro parse_header(header, type)
      if value = @arguments["{{ header.id }}"]?
        value.as?({{ type }}) || raise AvalancheMQ::Error::PreconditionFailed.new("{{ header.id }} header not a {{ type.id }}")
      end
    end

    private def validate_positive(header, value) : Nil
      return if value.nil?
      return if value >= 0
      raise AvalancheMQ::Error::PreconditionFailed.new("#{header} has to be positive")
    end

    private def validate_gt_zero(header, value) : Nil
      return if value.nil?
      return if value > 0
      raise AvalancheMQ::Error::PreconditionFailed.new("#{header} has to be larger than 0")
    end

    def immediate_delivery?
      @consumers.immediate_delivery?
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

    def pause!
      return unless @state == QueueState::Running
      @state = QueueState::Paused
    end

    def resume!
      return unless @state == QueueState::Paused
      @state = QueueState::Running
      @paused.send nil
    end

    def flow=(flow : Bool)
      if flow
        @state = QueueState::Running
        @paused.send nil
      else
        @state = QueueState::Flow
      end
    end

    private def deliver_loop
      i = 0
      loop do
        break if @state == QueueState::Closed
        if @ready.empty?
          i = 0
          receive_or_expire || break
        end
        if @state == QueueState::Running && (c = @consumers.next_consumer(i))
          deliver_to_consumer(c)
          # deliver 4096 msgs to a consumer then change consumer
          i = 0 if (i += 1) == 4096
        else
          break if @state == QueueState::Closed
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
          return true if @state == QueueState::Closed
          case ttl
          when q_ttl
            expire_queue && return false
          when m_ttl
            expire_messages
          else raise "Unknown TTL"
          end
        end
      elsif @state == QueueState::Flow || @state == QueueState::Paused
        @paused.receive
      else
        @consumer_available.receive
      end
      true
    end

    private def deliver_to_consumer(c)
      # @log.debug { "Getting a new message" }
      if env = get(c.no_ack, c)
        sp = env.segment_position
        # @log.debug { "Delivering #{sp} to consumer" }
        if c.deliver(env.message, sp, env.redelivered)
          if c.no_ack
            delete_message(sp)
          end
          if env.redelivered
            @redeliver_count += 1
          else
            @deliver_count += 1
          end
          # @log.debug { "Delivery done" }
        else
          @unacked.delete(sp)
          @ready.insert(sp)
          @log.debug { "Delivery failed, returning message to ready" }
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
      @consumers.cancel_consumers
      @consumers.clear
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
      @vhost.dirty = true
      @log.info { "(messages=#{message_count}) Deleted" }
      notify_observers(:delete)
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
        state: @state.to_s,
        effective_policy_definition: @policy,
        message_stats: stats_details,
        internal: @internal,
      }
    end

    class RejectOverFlow < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @state == QueueState::Closed
      # @log.debug { "Enqueuing message sp=#{sp}" }
      reject_on_overflow(sp)
      was_empty = @ready.push(sp) == 1
      drop_overflow unless immediate_delivery?
      @publish_count += 1
      if was_empty
        message_available
      elsif sp.expiration_ts > 0
        refresh_ttl_timeout
      end
      # @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} consumers=#{@consumers.size}" }
      true
    rescue Channel::ClosedError
      # if message_availabe channel is closed then abort
      false
    end

    private def reject_on_overflow(sp : SegmentPosition) : Nil
      return unless @reject_on_overflow
      if ml = @max_length
        if @ready.size >= ml
          @log.debug { "Overflow reject message sp=#{sp}" }
          raise RejectOverFlow.new
        end
      end

      if mlb = @max_length_bytes
        if @ready.sum(&.bytesize) + sp.bytesize >= mlb
          @log.debug { "Overflow reject message sp=#{sp}" }
          raise RejectOverFlow.new
        end
      end
    end

    private def drop_overflow : Nil
      if ml = @max_length
        @ready.limit_size(ml) do |sp|
          @log.debug { "Overflow drop head sp=#{sp}" }
          expire_msg(sp, :maxlen)
        end
      end

      if mlb = @max_length_bytes
        @ready.limit_byte_size(mlb) do |sp|
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
      @segment_id = id
      @segment_file = IO::Memory.new(mfile.to_slice, writeable: false)
    end

    def metadata(sp) : MessageMetadata?
      seg = segment_file(sp.segment)
      seg.seek(sp.position.to_i32, IO::Seek::Set)
      MessageMetadata.from_io(seg)
    rescue e : KeyError
      @log.error { "Segment file not found for #{sp}, removing index" }
      @ready.delete(sp)
      delete_message sp
      nil
    end

    private def time_to_message_expiration : Time::Span?
      sp = @ready.first? || return
      @log.debug { "Checking if message #{sp} has to be expired" }
      if expire_at = expire_at(sp)
        expire_in = expire_at - RoughTime.utc.to_unix_ms
        if expire_in > 0
          expire_in.milliseconds
        else
          Time::Span.zero
        end
      end
    end

    private def expire_at(sp : SegmentPosition) : Int64?
      if message_ttl = @message_ttl
        meta = metadata(sp)
        return nil unless meta
        expire_at = meta.timestamp + message_ttl
        if sp.expiration_ts > 0
          Math.min(expire_at, sp.expiration_ts)
        else
          expire_at
        end
      elsif sp.expiration_ts > 0
        sp.expiration_ts
      else
        nil
      end
    end

    private def expire_messages : Nil
      i = 0
      now = RoughTime.utc.to_unix_ms
      @ready.shift do |sp|
        @log.debug { "Checking if next message #{sp} has to be expired" }
        if expire_at = expire_at(sp)
          expire_in = expire_at - now
          @log.debug { "expire sp=#{sp} expire_in=#{expire_in}" }
          if expire_in > 0
            @log.debug { "No more message to expire" }
            break
          end

          expire_msg(sp, :expired)
          if (i += 1) == 8192
            Fiber.yield
            i = 0
          end
          true
        else
          break
        end
      end
      @log.info { "Expired #{i} messages" } if i > 0
    end

    private def expire_msg(sp : SegmentPosition, reason : Symbol)
      if sp.flags.has_dlx? || @dlx
        env = read(sp)
        expire_msg(env, reason)
      else
        delete_message sp
      end
    end

    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      if sp.flags.has_dlx? || @dlx
        dlx = msg.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
        if dlx
          unless dead_letter_loop?(msg.properties.headers, reason)
            dlrk = msg.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || msg.routing_key
            props = handle_dlx_header(msg, reason)
            dead_letter_msg(msg, sp, props, dlx, dlrk)
          end
        end
      end
      delete_message sp
    end

    # checks if the message has been dead lettered to the same queue
    # for the same reason already
    private def dead_letter_loop?(headers, reason) : Bool
      return false if headers.nil?
      if xdeaths = headers["x-death"]?.as?(Array(AMQ::Protocol::Field))
        xdeaths.each do |xd|
          if xd = xd.as?(AMQ::Protocol::Table)
            break if xd["reason"]? == "rejected"
            if xd["queue"]? == @name && xd["reason"]? == reason.to_s
              @log.debug { "preventing dead letter loop" }
              return true
            end
          end
        end
      end
      false
    end

    private def handle_dlx_header(meta, reason)
      props = meta.properties
      headers = props.headers ||= AMQP::Table.new

      headers.delete("x-delay")
      headers.delete("x-dead-letter-exchange")
      headers.delete("x-dead-letter-routing-key")

      # there's a performance advantage to do `has_key?` over `||=`
      headers["x-first-death-reason"] = reason.to_s unless headers.has_key? "x-first-death-reason"
      headers["x-first-death-queue"] = @name unless headers.has_key? "x-first-death-queue"
      headers["x-first-death-exchange"] = meta.exchange_name unless headers.has_key? "x-first-death-exchange"

      routing_keys = [meta.routing_key.as(AMQP::Field)]
      if cc = headers.delete("CC")
        # should route to all the CC RKs but then delete them,
        # so we (ab)use the BCC header for that
        headers["BCC"] = cc
        routing_keys.concat cc.as(Array(AMQP::Field))
      end

      xdeaths = headers["x-death"]?.as?(Array(AMQP::Field)) || Array(AMQP::Field).new(1)

      found_at = -1
      xdeaths.each_with_index do |xd, idx|
        xd = xd.as(AMQP::Table)
        next if xd["queue"]? != @name
        next if xd["reason"]? != reason.to_s
        next if xd["exchange"]? != meta.exchange_name

        count = xd["count"].as?(Int) || 0
        xd["count"] = count + 1
        xd["time"] = RoughTime.utc
        xd["routing_keys"] = routing_keys
        xd["original-expiration"] = props.expiration if props.expiration
        found_at = idx
        break
      end

      case found_at
      when -1
        # not found so inserting new x-death
        xd = AMQP::Table.new
        xd["queue"] = @name
        xd["reason"] = reason.to_s
        xd["exchange"] = meta.exchange_name
        xd["count"] = 1
        xd["time"] = RoughTime.utc
        xd["routing-keys"] = routing_keys
        xd["original-expiration"] = props.expiration if props.expiration
        xdeaths.unshift xd
      when 0
        # do nothing, updated xd is in the front
      else
        # move updated xd to the front
        xd = xdeaths.delete_at(found_at)
        xdeaths.unshift xd.as(AMQP::Table)
      end
      headers["x-death"] = xdeaths

      props.expiration = nil if props.expiration
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
      return nil unless @state == QueueState::Running
      @last_get_time = Time.monotonic
      @get_count += 1
      if env = get(no_ack)
        if no_ack
          delete_message(env.segment_position)
        end
        env
      end
    end

    # return the next message in the ready queue
    private def get(no_ack : Bool, consumer : Client::Channel::Consumer? = nil)
      return nil if @state == QueueState::Closed
      if sp = @ready.shift?
        unless no_ack
          @unacked.push(sp, consumer)
        end
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
      msg = BytesMessage.from_io(seg)
      redelivered = @requeued.includes?(sp)
      Envelope.new(sp, msg, redelivered)
    rescue ex : KeyError
      @log.error { "Segment file not found for #{sp}, removing index" }
      @ready.delete(sp)
      delete_message sp
      raise ex
    rescue ex
      @log.error { "Error reading message #{sp}, possible message loss. #{ex.inspect}" }
      raise ex
    ensure
      @requeued.delete(sp) if redelivered
    end

    def ack(sp : SegmentPosition) : Nil
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @ack_count += 1
      @unacked.delete(sp)
      delete_message(sp)
      consumer_available
    end

    protected def delete_message(sp : SegmentPosition) : Nil
      @deliveries.delete(sp) if @delivery_limit
      @vhost.dirty = true
    end

    def compact
      ready = @ready
      if ready.capacity > 1024 && ready.capacity > ready.size * 2
        elapsed = Time.measure do
          ready.compact
        end
        @log.info { "Compacting ready queue took #{elapsed.total_milliseconds} ms" }
      end
      unacked = @unacked
      if unacked.capacity > 1024 && unacked.capacity > unacked.size * 2
        elapsed = Time.measure do
          unacked.compact
        end
        @log.info { "Compacting unacked queue took #{elapsed.total_milliseconds} ms" }
      end
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted
      @log.debug { "Rejecting #{sp}, requeue: #{requeue}" }
      @unacked.delete(sp)
      if requeue
        was_empty = @ready.insert(sp) == 1
        @requeued << sp
        drop_overflow if @consumers.empty?
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
      drop_overflow if @consumers.empty?
      message_available if was_empty
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers.add_consumer(consumer)
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      consumer_available
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer, basic_cancel = false)
      deleted = @consumers.delete_consumer consumer
      consumer_unacked_size = @unacked.sum { |u| u.consumer == consumer ? 1 : 0 }
      unless basic_cancel
        requeue_many(@unacked.delete(consumer))
      end
      if deleted
        @exclusive_consumer = false if consumer.exclusive
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
      @vhost.dirty = true
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

    def to_json(builder : JSON::Builder)
      builder.object do
        details_tuple.each do |k, v|
          builder.field(k, v) unless v.nil?
          builder.field("consumer_details", @consumers)
        end
      end
    end

    class Error < Exception; end
  end
end
