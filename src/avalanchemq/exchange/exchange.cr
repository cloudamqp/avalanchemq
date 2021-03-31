require "logger"
require "../policy"
require "../stats"
require "../amqp"
require "../sortable_json"
require "../observable"
require "../queue"

module AvalancheMQ
  alias BindingKey = Tuple(String, Hash(String, AMQP::Field)?)
  alias Destination = Queue | Exchange

  abstract class Exchange
    include PolicyTarget
    include Stats
    include SortableJSON
    include Observable

    getter name, durable, auto_delete, internal, arguments, queue_bindings, exchange_bindings, vhost, type, alternate_exchange
    getter policy : Policy?
    getter? delayed = false

    @alternate_exchange : String?
    @persistent_queue : PersistentExchangeQueue?
    @delayed_queue : Queue?
    @log : Logger
    @deleted = false

    rate_stats(%w(publish_in publish_out unroutable))
    property publish_in_count, publish_out_count, unroutable_count

    def initialize(@vhost : VHost, @name : String, @durable = false,
                   @auto_delete = false, @internal = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @queue_bindings = Hash(BindingKey, Set(Queue)).new do |h, k|
        h[k] = Set(Queue).new
      end
      @exchange_bindings = Hash(BindingKey, Set(Exchange)).new do |h, k|
        h[k] = Set(Exchange).new
      end
      @log = @vhost.log.dup
      @log.progname += " exchange=#{@name}"
      handle_arguments
    end

    def apply_policy(policy : Policy)
      handle_arguments
      policy.not_nil!.definition.each do |k, v|
        @log.debug { "Applying policy #{k}: #{v}" }
        # TODO: Support persitent exchange as policy
        case k
        when "alternate-exchange"
          @alternate_exchange = v.as_s?
        when "delayed-message"
          @delayed = v.as?(Bool) == true
          init_delayed_queue if @delayed
        when "federation-upstream"
          @vhost.upstreams.try &.link(v.as_s, self)
        when "federation-upstream-set"
          @vhost.upstreams.try &.link_set(v.as_s, self)
        else nil
        end
      end
      @policy = policy
    end

    def clear_policy
      handle_arguments
      @policy = nil
    end

    def handle_arguments
      @alternate_exchange = (@arguments["x-alternate-exchange"]? || @arguments["alternate-exchange"]?).try &.to_s
      init_persistent_queue
      @delayed = @arguments["x-delayed-exchange"]?.try &.as?(Bool) == true
      init_delayed_queue if @delayed
    end

    def details_tuple
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        policy: @policy.try &.name, effective_policy_definition: @policy,
        message_stats: stats_details,
      }
    end

    def match?(frame : AMQP::Frame)
      type == frame.exchange_type &&
        @durable == frame.durable &&
        @auto_delete == frame.auto_delete &&
        @internal == frame.internal &&
        @arguments == frame.arguments.to_h
    end

    def match?(type, durable, auto_delete, internal, arguments)
      self.type == type &&
        @durable == durable &&
        @auto_delete == auto_delete &&
        @internal == internal &&
        @arguments == arguments.to_h
    end

    def in_use?
      return true if @queue_bindings.size > 0
      return true if @exchange_bindings.size > 0
      return true if @vhost.exchanges.any? { |_, x| x.exchange_bindings.any? { |_, exs| exs.includes? self } }
      false
    end

    def bindings_details
      @queue_bindings.each.chain(@exchange_bindings.each).flat_map do |(key, destinations)|
        destinations.map { |destination| binding_details(key, destination) }
      end
    end

    def binding_details(key, destination)
      BindingDetails.new(name, vhost.name, key, destination)
    end

    def persistent?
      !@persistent_queue.nil?
    end

    MAX_NAME_LENGTH = 256

    private def init_persistent_queue
      return if @persistent_queue
      persist_messages = @arguments["x-persist-messages"]?.try &.as?(ArgumentNumber)
      persist_ms = @arguments["x-persist-ms"]?.try &.as?(ArgumentNumber)
      return unless persist_messages || persist_ms
      raise "Exchange can't be persistent and delayed" if delayed?
      q_name = "amq.persistent.#{@name}"
      raise "Exchange name too long" if q_name.size > MAX_NAME_LENGTH
      args = Hash(String, AMQP::Field).new
      persist_messages.try do |n|
        next if n <= 0
        args["x-max-length"] = n
      end
      persist_ms.try do |ms|
        next if ms <= 0
        args["x-message-ttl"] = ms
      end
      @persistent_queue = QueueFactory.make_persistent_queue(@vhost, q_name, args)
      @vhost.queues[q_name] = @persistent_queue.not_nil!
    end

    private def init_delayed_queue
      return if @delayed_queue
      return unless @delayed
      raise "Exchange can't be persistent and delayed" if persistent?
      q_name = "amq.delayed.#{@name}"
      raise "Exchange name too long" if q_name.size > MAX_NAME_LENGTH
      @log.debug { "Declaring delayed queue: #{name}" }
      arguments = Hash(String, AMQP::Field){"x-dead-letter-exchange" => @name}
      @delayed_queue = QueueFactory.make_delayed_exchange_queue(@vhost, q_name, arguments)
      @vhost.queues[q_name] = @delayed_queue.as(Queue)
    end

    def referenced_sps(referenced_sps) : Nil
      if pq = @persistent_queue
        referenced_sps << VHost::SPQueue.new(pq.ready)
      end
      if dq = @delayed_queue
        referenced_sps << VHost::SPQueue.new(dq.ready)
      end
    end

    REPUBLISH_HEADERS = {"x-head", "x-tail", "x-from"}

    private def after_bind(destination : Destination, routing_key : String, headers : Hash(String, AMQP::Field)?)
      notify_observers(:bind, binding_details({routing_key, headers}, destination))
      if (pq = @persistent_queue) && headers && headers.any?
        method = headers.select(REPUBLISH_HEADERS).first_key?
        return unless method
        arg = headers[method].try &.as?(ArgumentNumber)
        return true unless arg && pq.any?
        persisted = pq.message_count
        @log.debug { "after_bind replaying persited message from #{method}-#{arg}, total_peristed: #{persisted}" }
        case destination
        when Queue
          republish(destination.as(Queue), method, arg)
        when Exchange
          raise "Not Implemented"
        end
      end
      true
    end

    private def republish(queue : Queue, method : String, arg : ArgumentNumber)
      return unless pq = @persistent_queue
      republish = ->(sp : SegmentPosition) do
        case type
        when "topic", "headers"
          if msg_metadata = queue.metadata(sp)
            properties = msg_metadata.properties
            rk = msg_metadata.routing_key
            headers = properties.headers
            queue_matches(rk, headers) do |mq|
              next unless mq == queue
              next unless queue.publish(sp)
              @publish_out_count += 1
            end
          end
        else
          return unless queue.publish(sp)
          @publish_out_count += 1
        end
      end
      case method
      when "x-head"
        pq.head(arg, &republish)
      when "x-tail"
        pq.tail(arg, &republish)
      when "x-from"
        pq.from(arg.to_i64, &republish)
      end
    end

    private def after_unbind(destination, routing_key, headers)
      @queue_bindings.reject! { |_k, v| v.empty? }
      @exchange_bindings.reject! { |_k, v| v.empty? }
      if @auto_delete &&
         @queue_bindings.each_value.all? &.empty? &&
         @exchange_bindings.each_value.all? &.empty?
        delete
      end
      notify_observers(:unbind, binding_details({routing_key, headers}, destination))
    end

    protected def delete
      return if @deleted
      @deleted = true
      @delayed_queue.try &.delete
      @persistent_queue.try &.delete
      @vhost.delete_exchange(@name)
      @log.info { "Deleted" }
      notify_observers(:delete)
    end

    abstract def type : String
    abstract def bind(destination : Queue, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def unbind(destination : Queue, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def bind(destination : Exchange, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def unbind(destination : Exchange, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def do_queue_matches(routing_key : String, headers : AMQP::Table?, &blk : Queue -> _)
    abstract def do_exchange_matches(routing_key : String, headers : AMQP::Table?, &blk : Exchange -> _)

    def queue_matches(routing_key : String, headers = nil, &blk : Queue -> _)
      if should_delay_message?(headers)
        @delayed_queue.try { |q| yield q }
      else
        do_queue_matches(routing_key, headers, &blk)
        @persistent_queue.try { |q| yield q } if persistent?
      end
    end

    private def should_delay_message?(headers)
      return false if headers.nil? || headers.empty?
      return false unless delayed?
      x_delay = headers["x-delay"]?
      return false unless x_delay
      x_deaths = headers["x-death"]?.try(&.as?(Array(AMQP::Field)))
      x_death = x_deaths.try(&.first).try(&.as?(AMQP::Table))
      should_delay = x_death.nil? || (x_death["queue"]? != "amq.delayed.#{@name}")
      @log.debug { "should_delay_message? #{should_delay}" }
      should_delay
    end

    def exchange_matches(routing_key : String, headers = nil, &blk : Exchange -> _)
      return if should_delay_message?(headers)
      do_exchange_matches(routing_key, headers, &blk)
    end
  end

  struct BindingDetails
    include SortableJSON
    getter source, vhost, key, destination

    def initialize(@source : String, @vhost : String,
                   @key : BindingKey, @destination : Queue | Exchange)
    end

    def routing_key
      @key[0]
    end

    def arguments
      @key[1]
    end

    def details_tuple
      {
        source:           @source,
        vhost:            @vhost,
        destination:      @destination.name,
        destination_type: @destination.is_a?(Queue) ? "queue" : "exchange",
        routing_key:      routing_key,
        arguments:        arguments,
        properties_key:   BindingDetails.hash_key(@key),
      }
    end

    def self.hash_key(key : BindingKey)
      if key[1].nil? || key[1].try &.empty?
        key[0].empty? ? "~" : key[0]
      else
        hsh = Base64.urlsafe_encode(key[1].to_s)
        "#{key[0]}~#{hsh}"
      end
    end
  end
end
