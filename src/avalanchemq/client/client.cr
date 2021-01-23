require "logger"
require "openssl"
require "socket"
require "../vhost"
require "../message"
require "./channel"
require "../user"
require "../stats"
require "../sortable_json"
require "../rough_time"
require "../error"
require "./amqp_connection"
require "../config"

module AvalancheMQ
  class Client
    include Stats
    include SortableJSON

    property direct_reply_consumer_tag
    getter vhost, channels, log, name
    getter user
    getter remote_address
    getter max_frame_size : UInt32
    getter channel_max : UInt16
    getter heartbeat_timeout : UInt16
    getter auth_mechanism : String
    getter client_properties : AMQP::Table
    getter direct_reply_consumer_tag : String?
    getter log : Logger

    @connected_at : Int64
    @heartbeat_interval : Time::Span?
    @running = true
    @last_recv_frame = RoughTime.utc
    @last_sent_frame = RoughTime.utc
    rate_stats(%w(send_oct recv_oct))
    DEFAULT_EX = "amq.default"

    def initialize(@socket : TCPSocket | OpenSSL::SSL::Socket | UNIXSocket,
                   @remote_address : Socket::IPAddress,
                   @local_address : Socket::IPAddress,
                   @vhost : VHost,
                   @user : User,
                   @events : Server::Event,
                   tune_ok,
                   start_ok)
      @log = vhost.log.dup
      @log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat_timeout = tune_ok.heartbeat
      @heartbeat_interval = tune_ok.heartbeat.zero? ? nil : (tune_ok.heartbeat / 2).seconds
      @auth_mechanism = start_ok.mechanism
      @name = "#{@remote_address} -> #{@local_address}"
      @client_properties = start_ok.client_properties
      @connected_at = Time.utc.to_unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @exclusive_queues = Array(Queue).new
      @vhost.add_connection(self)
      @events.send(EventType::ConnectionCreated)
      @log.info { "Connection (#{@name}) established for user=#{@user.name}" }
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    # socket's file descriptor
    def fd
      case @socket
      when OpenSSL::SSL::Socket
        @socket.as(OpenSSL::SSL::Socket).@bio.io.as(IO::FileDescriptor).fd
      when TCPSocket
        @socket.as(TCPSocket).fd
      when UNIXSocket
        @socket.as(UNIXSocket).fd
      else
        raise "Unexpected socket #{@socket.class}"
      end
    end

    def channel_name_prefix
      @remote_address.to_s
    end

    def details_tuple
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @channel_max,
        frame_max:         @max_frame_size,
        timeout:           @heartbeat_timeout,
        client_properties: @client_properties,
        vhost:             @vhost.name,
        user:              @user.name,
        protocol:          "AMQP 0-9-1",
        auth_mechanism:    @auth_mechanism,
        host:              @local_address.address,
        port:              @local_address.port,
        peer_host:         @remote_address.address,
        peer_port:         @remote_address.port,
        name:              @name,
        pid:               @name,
        ssl:               tls_terminated?,
        tls_version:       tls_version,
        cipher:            cipher,
        state:             state,
      }.merge(stats_details)
    end

    private def read_loop
      i = 0
      socket = @socket
      loop do
        AMQP::Frame.from_io(socket) do |frame|
          {% unless flag?(:release) %}
            @log.debug { "Received #{frame.inspect}" }
          {% end %}
          if (i += 1) == 8192
            i = 0
            Fiber.yield
          end
          frame_size_ok?(frame) || return
          case frame
          when AMQP::Frame::Connection::Close
            send AMQP::Frame::Connection::CloseOk.new
            return
          when AMQP::Frame::Connection::CloseOk
            @log.debug "Confirmed disconnect"
            return
          end
          if @running
            process_frame(frame)
          else
            case frame
            when AMQP::Frame::Body
              @log.debug { "Skipping body, waiting for CloseOk" }
              frame.body.skip(frame.body_size)
            else
              @log.debug { "Discarding #{frame.class.name}, waiting for CloseOk" }
            end
          end
        rescue e : Error::PreconditionFailed
          send_precondition_failed(frame, e.message)
        end
      rescue IO::TimeoutError
        unless send_heartbeat
          cleanup
          close_socket
          break
        end
      rescue ex : AMQP::Error::NotImplemented
        @log.error { ex.inspect }
        send_not_implemented(ex)
      rescue ex : AMQP::Error::FrameDecode
        @log.error { ex.inspect }
        send_frame_error
        return
      rescue ex : IO::Error | OpenSSL::SSL::Error
        @log.debug { "Lost connection, while reading (#{ex.inspect_with_backtrace})" } unless closed?
        cleanup
        close_socket
        return
      rescue ex : Exception
        @log.error { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
        send_internal_error(ex.message)
        return
      end
    ensure
      @log.info "Connection (#{@name}) disconnected for user=#{@user.name} "
    end

    private def frame_size_ok?(frame) : Bool
      if frame.bytesize > @max_frame_size
        send_frame_error("frame size #{frame.bytesize} exceeded max #{@max_frame_size} bytes")
        return false
      end
      true
    end

    private def send_heartbeat
      now = RoughTime.utc
      if @last_recv_frame + (@heartbeat_timeout + 5).seconds < now
        recv_ago = (now - @last_recv_frame).total_seconds.round(1)
        sent_ago = (now - @last_sent_frame).total_seconds.round(1)
        @log.info { "Heartbeat timeout (#{@heartbeat_timeout}), last seen frame #{recv_ago} s ago, sent frame #{sent_ago} s ago" }
        false
      else
        send AMQP::Frame::Heartbeat.new
      end
    end

    def send(frame : AMQP::Frame) : Bool
      return false if closed?
      {% unless flag?(:release) %}
        @log.debug { "Send #{frame.inspect}" }
      {% end %}
      @write_lock.synchronize do
        s = @socket
        s.write_bytes frame, IO::ByteFormat::NetworkEndian
        s.flush
      end
      @last_sent_frame = RoughTime.utc
      @send_oct_count += 8_u64 + frame.bytesize
      if frame.is_a?(AMQP::Frame::Connection::CloseOk)
        cleanup
        close_socket
        return false
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost connection, while sending (#{ex.inspect})" } unless closed?
      cleanup
      false
    rescue ex : IO::TimeoutError
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send_internal_error(ex.message)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    @write_lock = Mutex.new(:unchecked)

    def deliver(frame, msg)
      @write_lock.synchronize do
        socket = @socket
        {% unless flag?(:release) %}
          @log.debug { "Send #{frame.inspect}" }
        {% end %}
        socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + frame.bytesize
        header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
        {% unless flag?(:release) %}
          @log.debug { "Send #{header.inspect}" }
        {% end %}
        socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + header.bytesize
        pos = 0
        while pos < msg.size
          length = Math.min(msg.size - pos, @max_frame_size - 8).to_u32
          {% unless flag?(:release) %}
            @log.debug { "Send BodyFrame (pos #{pos}, length #{length})" }
          {% end %}
          body = case msg
                 in BytesMessage
                   AMQP::Frame::BytesBody.new(frame.channel, length, msg.body[pos, length])
                 in Message
                   AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
                 end
          socket.write_bytes body, ::IO::ByteFormat::NetworkEndian
          @send_oct_count += 8_u64 + body.bytesize
          pos += length
        end
        socket.flush
        @last_sent_frame = RoughTime.utc
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | AMQ::Protocol::Error::FrameEncode
      @log.debug { "Lost connection, while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex : IO::TimeoutError
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Delivery exception: #{ex.inspect_with_backtrace}" }
      raise ex
    end

    def state
      !@running ? "closed" : (@vhost.flow? ? "running" : "flow")
    end

    def self.start(socket, remote_address, local_address, vhosts, users, log, events)
      AMQPConnection.start(socket, remote_address, local_address, vhosts, users, log.dup, events)
    end

    private def with_channel(frame)
      if ch = @channels[frame.channel]?
        if ch.running?
          yield ch
        else
          case frame
          when AMQP::Frame::Basic::Publish, AMQP::Frame::Header
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          when AMQP::Frame::Body
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            frame.body.skip(frame.body_size)
          else
            yield ch
          end
        end
      else
        case frame
        when AMQP::Frame::Body
          @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          frame.body.skip(frame.body_size)
        end
        @log.error { "Channel #{frame.channel} not open" }
        close_connection(frame, 504_u16, "CHANNEL_ERROR - Channel #{frame.channel} not open")
      end
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel, @events)
      send AMQP::Frame::Channel::OpenOk.new(frame.channel)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def process_frame(frame) : Nil
      @last_recv_frame = now = RoughTime.utc
      @recv_oct_count += 8_u64 + frame.bytesize
      case frame
      when AMQP::Frame::Channel::Open
        open_channel(frame)
      when AMQP::Frame::Channel::Close
        @channels.delete(frame.channel).try &.close
        send AMQP::Frame::Channel::CloseOk.new(frame.channel)
      when AMQP::Frame::Channel::CloseOk
        @channels.delete(frame.channel).try &.close
      when AMQP::Frame::Channel::Flow
        @channels[frame.channel].client_flow(frame.active)
      when AMQP::Frame::Channel::FlowOk
        # noop
      when AMQP::Frame::Confirm::Select
        with_channel frame, &.confirm_select(frame)
      when AMQP::Frame::Exchange::Declare
        declare_exchange(frame)
      when AMQP::Frame::Exchange::Delete
        delete_exchange(frame)
      when AMQP::Frame::Exchange::Bind
        bind_exchange(frame)
      when AMQP::Frame::Exchange::Unbind
        unbind_exchange(frame)
      when AMQP::Frame::Queue::Declare
        declare_queue(frame)
      when AMQP::Frame::Queue::Bind
        bind_queue(frame)
      when AMQP::Frame::Queue::Unbind
        unbind_queue(frame)
      when AMQP::Frame::Queue::Delete
        delete_queue(frame)
      when AMQP::Frame::Queue::Purge
        purge_queue(frame)
      when AMQP::Frame::Basic::Publish
        start_publish(frame)
      when AMQP::Frame::Header
        with_channel frame, &.next_msg_headers(frame, now)
      when AMQP::Frame::Body
        with_channel frame, &.add_content(frame, now)
      when AMQP::Frame::Basic::Consume
        consume(frame)
      when AMQP::Frame::Basic::Get
        basic_get(frame)
      when AMQP::Frame::Basic::Ack
        with_channel frame, &.basic_ack(frame)
      when AMQP::Frame::Basic::Reject
        with_channel frame, &.basic_reject(frame)
      when AMQP::Frame::Basic::Nack
        with_channel frame, &.basic_nack(frame)
      when AMQP::Frame::Basic::Cancel
        with_channel frame, &.cancel_consumer(frame)
      when AMQP::Frame::Basic::Qos
        with_channel frame, &.basic_qos(frame)
      when AMQP::Frame::Basic::Recover
        with_channel frame, &.basic_recover(frame)
      when AMQP::Frame::Heartbeat
        nil
      else
        @log.error { "#{frame.inspect}, not implemented" }
        send_not_implemented(frame)
      end
      if heartbeat_interval = @heartbeat_interval
        if @last_sent_frame + heartbeat_interval < now
          send AMQP::Frame::Heartbeat.new
        end
      end
    rescue frame : Error::UnexpectedFrame
      @log.error { "#{frame.inspect}, unexpected frame" }
      close_channel(frame, 505_u16, "UNEXPECTED_FRAME")
    end

    private def cleanup
      @running = false
      @exclusive_queues.each(&.close)
      @exclusive_queues.clear
      @channels.each_value &.close
      @channels.clear
      @events.send(EventType::ConnectionClosed) unless @events.closed?
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    private def close_socket
      @socket.close
      @log.debug { "Socket closed" }
    rescue ex
      @log.debug { "#{ex.inspect} when closing socket" }
    end

    def close(reason = nil)
      reason ||= "Connection closed"
      @log.info { "Closing, #{reason}" }
      close_frame = AMQP::Frame::Connection::Close.new(320_u16, reason.to_s, 0_u16, 0_u16)
      send(close_frame) || cleanup
      @running = false
    end

    def force_close
      close_socket
    end

    def closed?
      !@running
    end

    def close_channel(frame, code, text)
      return close_connection(frame, code, text) if frame.channel.zero?
      case frame
      when AMQ::Protocol::Frame::Method
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, frame.class_id, frame.method_id)
      else
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, 0, 0)
      end
      @channels[frame.channel].running = false
    end

    def close_connection(frame, code, text)
      @log.info { "Closing, #{text}" }
      case frame
      when AMQ::Protocol::Frame::Method
        send AMQP::Frame::Connection::Close.new(code, text, frame.class_id, frame.method_id)
      else
        send AMQP::Frame::Connection::Close.new(code, text, 0_u16, 0_u16)
      end
      @log.info { "Connection=#{@name} disconnected" }
      @running = false
    end

    def direct_reply_channel
      if direct_reply_consumer_tag
        @vhost.direct_reply_channels[direct_reply_consumer_tag]?
      end
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def send_access_refused(frame, text)
      @log.warn { "Access refused channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 403_u16, "ACCESS_REFUSED - #{text}")
    end

    def send_not_found(frame, text = "")
      @log.warn { "Not found channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 404_u16, "NOT_FOUND - #{text}")
    end

    def send_resource_locked(frame, text)
      @log.warn { "Resource locked channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 405_u16, "RESOURCE_LOCKED - #{text}")
    end

    def send_precondition_failed(frame, text)
      @log.warn { "Precondition failed channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 406_u16, "PRECONDITION_FAILED - #{text}")
    end

    def send_not_implemented(frame)
      @log.error { "#{frame.inspect}, not implemented" }
      close_connection(frame, 540_u16, "NOT_IMPLEMENTED")
    end

    def send_internal_error(message)
      send AMQP::Frame::Connection::Close.new(541_u16, "INTERNAL_ERROR - #{message}", 0_u16, 0_u16)
    end

    def send_frame_error(message = nil)
      send AMQP::Frame::Connection::Close.new(501_u16, "FRAME_ERROR - #{message}", 0_u16, 0_u16)
    end

    private def declare_exchange(frame)
      if !valid_entity_name(frame.exchange_name)
        send_precondition_failed(frame, "Exchange name isn't valid")
      elsif frame.exchange_name.empty?
        send_access_refused(frame, "Not allowed to declare the default exchange")
      elsif e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.passive || e.match?(frame)
          unless frame.no_wait
            send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_precondition_failed(frame, "Existing exchange '#{frame.exchange_name}' declared with other arguments")
        end
      elsif frame.passive
        send_not_found(frame, "Exchange '#{frame.exchange_name}' doesn't exists")
      elsif frame.exchange_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        ae = frame.arguments["x-alternate-exchange"]?.try &.as?(String)
        ae_ok = ae.nil? || (@user.can_write?(@vhost.name, ae) && @user.can_read?(@vhost.name, frame.exchange_name))
        unless @user.can_config?(@vhost.name, frame.exchange_name) && ae_ok
          send_access_refused(frame, "User doesn't have permissions to declare exchange '#{frame.exchange_name}'")
          return
        end
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeclareOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_exchange(frame)
      if !valid_entity_name(frame.exchange_name)
        send_precondition_failed(frame, "Exchange name isn't valid")
      elsif frame.exchange_name.empty?
        send_access_refused(frame, "Not allowed to delete the default exchange")
      elsif frame.exchange_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        # should return not_found according to spec but we make it idempotent
        send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
      elsif !@user.can_config?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
      elsif frame.if_unused && @vhost.exchanges[frame.exchange_name].in_use?
        send_precondition_failed(frame, "Exchange '#{frame.exchange_name}' in use")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def delete_queue(frame)
      if frame.queue_name.empty? && @last_queue_name
        frame.queue_name = @last_queue_name.not_nil!
      end
      if !valid_entity_name(frame.queue_name)
        send_precondition_failed(frame, "Queue name isn't valid")
        return
      end
      q = @vhost.queues.fetch(frame.queue_name, nil)
      if q.nil?
        send AMQP::Frame::Queue::DeleteOk.new(frame.channel, 0_u32) unless frame.no_wait
      elsif queue_exclusive_to_other_client?(q)
        send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
      elsif frame.if_unused && !q.consumer_count.zero?
        send_precondition_failed(frame, "Queue '#{q.name}' in use")
      elsif frame.if_empty && !q.message_count.zero?
        send_precondition_failed(frame, "Queue '#{q.name}' is not empty")
      elsif !@user.can_config?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have permissions to delete queue '#{q.name}'")
      elsif q.internal?
        send_access_refused(frame, "Not allowed to delete internal queue")
      else
        size = q.message_count
        @vhost.apply(frame)
        @exclusive_queues.delete(q) if q.exclusive
        send AMQP::Frame::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
      end
    end

    private def valid_entity_name(name) : Bool
      return true if name.empty?
      name.matches?(/\A[ -~]*\z/)
    end

    def queue_exclusive_to_other_client?(q)
      q.exclusive && !@exclusive_queues.includes?(q)
    end

    private def invalid_exclusive_redeclare?(frame, q)
      !(frame.passive || frame.exclusive || !q.exclusive)
    end

    private def declare_queue(frame)
      if !frame.queue_name.empty? && !valid_entity_name(frame.queue_name)
        send_precondition_failed(frame, "Queue name isn't valid")
      elsif q = @vhost.queues.fetch(frame.queue_name, nil)
        redeclare_queue(frame, q)
      elsif frame.passive
        send_not_found(frame, "Queue '#{frame.queue_name}' doesn't exists")
      elsif frame.queue_name =~ /^amq\.(rabbitmq|direct)\.reply-to/
        unless frame.no_wait
          consumer_count = direct_reply_channel.nil? ? 0_u32 : 1_u32
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, consumer_count)
        end
      elsif frame.queue_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        declare_new_queue(frame)
      end
    end

    private def redeclare_queue(frame, q)
      if queue_exclusive_to_other_client?(q) || invalid_exclusive_redeclare?(frame, q)
        send_resource_locked(frame, "Exclusive queue")
      elsif q.internal?
        send_access_refused(frame, "Queue '#{frame.queue_name}' in vhost '#{@vhost.name}' is internal")
      elsif frame.passive || q.match?(frame)
        q.redeclare
        unless frame.no_wait
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, q.name,
            q.message_count, q.consumer_count)
        end
        @last_queue_name = frame.queue_name
      else
        send_precondition_failed(frame, "Existing queue '#{q.name}' declared with other arguments")
      end
    end

    @last_queue_name : String?

    private def declare_new_queue(frame)
      if frame.queue_name.empty?
        frame.queue_name = Queue.generate_name
      end
      dlx = frame.arguments["x-dead-letter-exchange"]?.try &.as?(String)
      dlx_ok = dlx.nil? || (@user.can_write?(@vhost.name, dlx) && @user.can_read?(@vhost.name, name))
      unless @user.can_config?(@vhost.name, frame.queue_name) && dlx_ok
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue_name}'")
        return
      end
      @vhost.apply(frame)
      @last_queue_name = frame.queue_name
      if frame.exclusive
        @exclusive_queues << @vhost.queues[frame.queue_name]
      end
      unless frame.no_wait
        send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
      end
    end

    private def bind_queue(frame)
      if frame.queue_name.empty? && @last_queue_name
        frame.queue_name = @last_queue_name.not_nil!
        # according to spec if both queue name and routing key is empty,
        # then substitute them with the name of the last declared queue
        if frame.routing_key.empty?
          frame.routing_key = @last_queue_name.not_nil!
        end
      end
      return unless valid_q_bind_unbind?(frame)

      q = @vhost.queues.fetch(frame.queue_name, nil)
      if q.nil?
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      elsif @vhost.queues.fetch(frame.queue_name, nil).try &.internal?
        send_access_refused(frame, "Not allowed to bind to internal queue")
      elsif queue_exclusive_to_other_client?(q)
        send_resource_locked(frame, "Exclusive queue")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_queue(frame)
      if frame.queue_name.empty? && @last_queue_name
        frame.queue_name = @last_queue_name.not_nil!
      end
      return unless valid_q_bind_unbind?(frame)

      q = @vhost.queues.fetch(frame.queue_name, nil)
      if q.nil?
        # should return not_found according to spec but we make it idempotent
        send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        # should return not_found according to spec but we make it idempotent
        send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      elsif @vhost.queues.fetch(frame.queue_name, nil).try &.internal?
        send_access_refused(frame, "Not allowed to unbind from the internal queue")
      elsif queue_exclusive_to_other_client?(q)
        send_resource_locked(frame, "Exclusive queue")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
      end
    end

    private def valid_q_bind_unbind?(frame) : Bool
      if !valid_entity_name(frame.queue_name)
        send_precondition_failed(frame, "Queue name isn't valid")
        return false
      elsif !valid_entity_name(frame.exchange_name)
        send_precondition_failed(frame, "Exchange name isn't valid")
        return false
      elsif frame.exchange_name.empty? || frame.exchange_name == DEFAULT_EX
        send_access_refused(frame, "Not allowed to unbind from the default exchange")
        return false
      end
      true
    end

    private def bind_exchange(frame)
      source = @vhost.exchanges.fetch(frame.source, nil)
      destination = @vhost.exchanges.fetch(frame.destination, nil)
      if destination.nil?
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif source.nil?
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      elsif frame.source.empty? || frame.destination.empty?
        send_access_refused(frame, "Not allowed to bind to the default exchange")
      elsif source.try(&.persistent?)
        send_access_refused(frame, "Not allowed to bind persistent exchange to exchange")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_exchange(frame)
      source = @vhost.exchanges.fetch(frame.source, nil)
      destination = @vhost.exchanges.fetch(frame.destination, nil)
      if destination.nil?
        # should return not_found according to spec but we make it idempotent
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel)
      elsif source.nil?
        # should return not_found according to spec but we make it idempotent
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel)
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      elsif frame.source.empty? || frame.destination.empty? || frame.source == DEFAULT_EX || frame.destination == DEFAULT_EX
        send_access_refused(frame, "Not allowed to unbind from the default exchange")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      if frame.queue_name.empty? && @last_queue_name
        frame.queue_name = @last_queue_name.not_nil!
      end
      unless @user.can_read?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        return
      end
      if !valid_entity_name(frame.queue_name)
        send_precondition_failed(frame, "Queue name isn't valid")
      elsif q = @vhost.queues.fetch(frame.queue_name, nil)
        if queue_exclusive_to_other_client?(q)
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        else
          messages_purged = q.purge
          send AMQP::Frame::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
        end
      else
        send_not_found(frame, "Queue '#{frame.queue_name}' not found")
      end
    end

    private def start_publish(frame)
      unless @user.can_write?(@vhost.name, frame.exchange)
        send_access_refused(frame, "User not allowed to publish to exchange '#{frame.exchange}'")
        return
      end
      with_channel frame, &.start_publish(frame)
    end

    private def consume(frame)
      if frame.queue.empty? && @last_queue_name
        frame.queue = @last_queue_name.not_nil!
      end
      if !valid_entity_name(frame.queue)
        send_precondition_failed(frame, "Queue name isn't valid")
        return
      end
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
        return
      end
      with_channel frame, &.consume(frame)
    end

    private def basic_get(frame)
      if frame.queue.empty? && @last_queue_name
        frame.queue = @last_queue_name.not_nil!
      end
      if !valid_entity_name(frame.queue)
        send_precondition_failed(frame, "Queue name isn't valid")
        return
      end
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
        return
      end
      # yield so that msg expiration, consumer delivery etc gets priority
      Fiber.yield
      with_channel frame, &.basic_get(frame)
    end

    private def tls_terminated?
      @socket.is_a?(OpenSSL::SSL::Socket) ||
        (@socket.is_a?(UNIXSocket) && Config.instance.unix_socket_tls_terminated)
    end

    private def tls_version
      return @socket.as(OpenSSL::SSL::Socket).tls_version if @socket.is_a?(OpenSSL::SSL::Socket)
      return "Unknown" if @socket.is_a?(UNIXSocket) && Config.instance.unix_socket_tls_terminated
      nil
    end

    private def cipher
      return @socket.as(OpenSSL::SSL::Socket).cipher if @socket.is_a?(OpenSSL::SSL::Socket)
      return "Unknown" if @socket.is_a?(UNIXSocket) && Config.instance.unix_socket_tls_terminated
      nil
    end

    def save_to_json(json)
      json.object do
        json.field "remote_address", @remote_address.to_s # or take these from the new socket?
        json.field "local_address", @local_address.to_s # or take these from the new socket?
        json.field "user", @user.name
        json.field "channel_max", @channel_max
        json.field "frame_max", @max_frame_size
        json.field "heartbeat_timeout", @heartbeat_timeout
        json.field "properties" do
          @client_properties.to_json(json)
        end
        json.field "channels" do
          json.array do
            @channels.each_value do |ch|
              ch.save_to_json(json)
            end
          end
        end
      end
    end

    def self.restore_from_json(json)
      conn = json.as_h
      username = conn["user"].as_s
      channel_max = conn["channel_max"].as_i.to_u16
      properties = conn["properties"].as_h
      Client.new(...)
    end
  end
end
