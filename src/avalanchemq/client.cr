require "socket"
require "logger"
require "./message"
require "./client/*"

module AvalancheMQ
  class Client
    getter socket, vhost, user, channels, log, max_frame_size, exclusive_queues

    @log : Logger

    def initialize(@socket : TCPSocket | OpenSSL::SSL::Socket,
                   @remote_address : Socket::IPAddress,
                   @vhost : VHost,
                   @user : User,
                   @max_frame_size : UInt32)
      @log = @vhost.log.dup
      @log.progname += "/Client[#{@remote_address}]"
      @log.info "Connected"
      @channels = Hash(UInt16, Client::Channel).new
      @exclusive_queues = Array(Queue).new
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def self.start(socket, remote_address, vhosts, users, log)
      proto = uninitialized UInt8[8]
      bytes = socket.read_fully(proto.to_slice)

      if proto != AMQP::PROTOCOL_START
        socket.write AMQP::PROTOCOL_START.to_slice
        socket.close
        return
      end

      start = AMQP::Connection::Start.new
      socket.write start.to_slice
      socket.flush
      start_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::StartOk)
      _, username, password = start_ok.response.split("\u0000")
      user = users[username]?
      unless user && user.password == password
        log.warn "Access denied for #{remote_address} using username \"#{username}\""
        props = start_ok.client_properties
        capabilities = props["capabilities"]?.try &.as(Hash(String, AMQP::Field))
        if capabilities && capabilities["authentication_failure_close"].try &.as(Bool)
          socket.write AMQP::Connection::Close.new(403_u16, "ACCESS_REFUSED",
                                                   start_ok.class_id,
                                                   start_ok.method_id).to_slice
          socket.flush
          AMQP::Frame.decode(socket).as(AMQP::Connection::CloseOk)
        end
        socket.close
        return
      end
      socket.write AMQP::Connection::Tune.new(channel_max: 0_u16,
                                              frame_max: 131072_u32,
                                              heartbeat: 0_u16).to_slice
      socket.flush
      tune_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::TuneOk)
      open = AMQP::Frame.decode(socket).as(AMQP::Connection::Open)
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write AMQP::Connection::OpenOk.new.to_slice
          socket.flush
          return self.new(socket, remote_address, vhost, user, tune_ok.frame_max)
        else
          log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
          reply_text = "ACCESS_REFUSED - '#{username}' doesn't have access to '#{vhost.name}'"
          socket.write AMQP::Connection::Close.new(403_u16, reply_text,
                                                   open.class_id, open.method_id).to_slice
          socket.flush
          AMQP::Frame.decode(socket).as(AMQP::Connection::CloseOk)
          socket.close
        end
      else
        log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
        socket.write AMQP::Connection::Close.new(402_u16, "INVALID_PATH - vhost not found",
                                                 open.class_id, open.method_id).to_slice
        socket.flush
        AMQP::Frame.decode(socket).as(AMQP::Connection::CloseOk)
        socket.close
      end
      nil
    rescue ex : AMQP::FrameDecodeError
      log.warn "#{ex.cause.inspect} while #{remote_address} tried to establish connection"
      nil
    rescue ex : Exception
      log.warn "#{ex.inspect} while #{remote_address} tried to establish connection"
      socket.close unless socket.closed?
      nil
    end

    def close
      @log.debug "Gracefully closing"
      send AMQP::Connection::Close.new(320_u16, "Broker shutdown", 0_u16, 0_u16)
    end

    def cleanup
      @log.debug "Yielding before cleaning up"
      Fiber.yield
      @log.debug "Cleaning up"
      @exclusive_queues.each &.close
      @channels.each_value &.close
      @channels.clear
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def to_json(json : JSON::Builder)
      {
        address: @remote_address.to_s,
        channels: @channels.size,
      }.to_json(json)
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
      send AMQP::Channel::OpenOk.new(frame.channel)
    end

    private def declare_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.passive ||
            e.type == frame.exchange_type &&
            e.durable == frame.durable &&
            e.auto_delete == frame.auto_delete &&
            e.internal == frame.internal &&
            e.arguments == frame.arguments
          unless frame.no_wait
            send AMQP::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_access_refused(frame, "Existing exchange declared with other arguments")
        end
      elsif frame.passive
        send_not_found(frame)
      elsif frame.exchange_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        unless can_config? frame.exchange_name
          send_access_refused(frame, "User doesn't have permissions to declare exchange '#{frame.exchange_name}'")
          return
        end
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        end
      end
    end

    private def delete_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.exchange_name.starts_with? "amq."
          send_access_refused(frame, "Not allowed to use the amq. prefix")
          return
        elsif !can_config?(frame.exchange_name)
          send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
        else
          @vhost.apply(frame)
          unless frame.no_wait
            send AMQP::Exchange::DeleteOk.new(frame.channel)
          end
        end
      else
        send_not_found(frame)
      end
    end

    private def delete_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.if_unused && !q.consumer_count.zero?
          send_precondition_failed(frame, "In use")
        elsif frame.if_empty && !q.message_count.zero?
          send_precondition_failed(frame, "Not empty")
        elsif !can_config?(frame.queue_name)
          send_access_refused(frame, "User doesn't have permissions to delete queue '#{frame.queue_name}'")
        else
          size = q.message_count
          q.delete
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive
          send AMQP::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
        end
      else
        send_not_found(frame)
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.passive ||
          q.durable == frame.durable &&
          q.exclusive == frame.exclusive &&
          q.auto_delete == frame.auto_delete &&
          q.arguments == frame.arguments
          unless frame.no_wait
            send AMQP::Queue::DeclareOk.new(frame.channel, q.name,
                                            q.message_count, q.consumer_count)
          end
        else
          send_access_refused(frame, "Existing queue declared with other arguments")
        end
      elsif frame.passive
        send_not_found(frame)
      elsif frame.queue_name.starts_with? "amq."
        send_access_refused(frame, "Forbidden to use the prefix amq.")
      else
        if frame.queue_name.empty?
          frame.queue_name = "amq.gen-#{Random::Secure.urlsafe_base64(24)}"
        end
        unless can_config? frame.queue_name
          send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue_name}'")
          return
        end
        @vhost.apply(frame)
        if frame.exclusive
          @exclusive_queues << @vhost.queues[frame.queue_name]
        end
        unless frame.no_wait
          send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
        end
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      elsif !can_read? frame.exchange_name
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !can_write? frame.queue_name
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      else
        @vhost.apply(frame)
        send AMQP::Queue::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      elsif !can_read? frame.exchange_name
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !can_write? frame.queue_name
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      else
        @vhost.apply(frame)
        send AMQP::Queue::UnbindOk.new(frame.channel)
      end
    end

    private def bind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      elsif !can_read? frame.source
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !can_write? frame.destination
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Exchange::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      elsif !can_read? frame.source
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !can_write? frame.destination
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      unless can_read? frame.queue_name
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        return
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        else
          messages_purged = q.purge
          send AMQP::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
        end
      else
        send_not_found(frame, "Queue #{frame.queue_name} not found")
      end
    end

    private def read_loop
      i = 0
      loop do
        frame = AMQP::Frame.decode @socket
        @log.debug { "Read #{frame.inspect}" }
        ok = process_frame(frame)
        break unless ok
        Fiber.yield if (i += 1) % 1000 == 0
      end
    rescue ex : AMQP::NotImplemented
      @log.error { "#{ex} when reading from socket" }
      if ex.channel > 0
        send AMQP::Channel::Close.new(ex.channel, 540_u16, "Not implemented", ex.class_id, ex.method_id)
      else
        send AMQP::Connection::Close.new(540_u16, "Not implemented", ex.class_id, ex.method_id)
      end
    rescue ex : AMQP::FrameDecodeError
      @log.info "Lost connection, while reading (#{ex.cause})"
      cleanup
    rescue ex : Exception
      @log.error { "Unexpected error, while reading: #{ex.inspect}" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    private def process_frame(frame)
      case frame
      when AMQP::Connection::Close
        send AMQP::Connection::CloseOk.new
        return false
      when AMQP::Connection::CloseOk
        @log.info "Disconnected"
        @log.debug { "Closing socket" }
        @socket.close
        cleanup
        return false
      when AMQP::Channel::Open
        open_channel(frame)
      when AMQP::Channel::Close
        @channels.delete(frame.channel).try &.close
        send AMQP::Channel::CloseOk.new(frame.channel)
      when AMQP::Channel::CloseOk
        @channels.delete(frame.channel).try &.close
      when AMQP::Confirm::Select
        @channels[frame.channel].confirm_select(frame)
      when AMQP::Exchange::Declare
        declare_exchange(frame)
      when AMQP::Exchange::Delete
        delete_exchange(frame)
      when AMQP::Exchange::Bind
        bind_exchange(frame)
      when AMQP::Exchange::Unbind
        unbind_exchange(frame)
      when AMQP::Queue::Declare
        declare_queue(frame)
      when AMQP::Queue::Bind
        bind_queue(frame)
      when AMQP::Queue::Unbind
        unbind_queue(frame)
      when AMQP::Queue::Delete
        delete_queue(frame)
      when AMQP::Queue::Purge
        purge_queue(frame)
      when AMQP::Basic::Publish
        @channels[frame.channel].start_publish(frame)
      when AMQP::HeaderFrame
        @channels[frame.channel].next_msg_headers(frame)
      when AMQP::BodyFrame
        @channels[frame.channel].add_content(frame)
      when AMQP::Basic::Consume
        @channels[frame.channel].consume(frame)
      when AMQP::Basic::Get
        @channels[frame.channel].basic_get(frame)
      when AMQP::Basic::Ack
        @channels[frame.channel].basic_ack(frame)
      when AMQP::Basic::Reject
        @channels[frame.channel].basic_reject(frame)
      when AMQP::Basic::Nack
        @channels[frame.channel].basic_nack(frame)
      when AMQP::Basic::Cancel
        @channels[frame.channel].cancel_consumer(frame)
      when AMQP::Basic::Qos
        @channels[frame.channel].basic_qos(frame)
      when AMQP::HeartbeatFrame
        send AMQP::HeartbeatFrame.new
      else
        raise AMQP::NotImplemented.new(frame)
      end
      true
    rescue ex : AMQP::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      raise ex if ex.channel == 0
      send AMQP::Channel::Close.new(ex.channel, 540_u16, "NOT_IMPLEMENTED", ex.class_id, ex.method_id)
      true
    rescue ex : KeyError
      raise ex unless frame.is_a? AMQP::MethodFrame
      @log.error { "Channel #{frame.channel} not open" }
      send AMQP::Connection::Close.new(504_u16, "CHANNEL_ERROR - Channel #{frame.channel} not open",
                                       frame.class_id, frame.method_id)
      true
    rescue ex : Exception
      raise ex unless frame.is_a? AMQP::MethodFrame
      @log.error { "#{ex.inspect}, when processing frame" }
      send AMQP::Channel::Close.new(frame.channel, 541_u16, "INTERNAL_ERROR",
                                    frame.class_id, frame.method_id)
      true
    end

    def send_access_refused(frame, text)
      reply_text = "ACCESS_REFUSED - #{text}"
      send AMQP::Channel::Close.new(frame.channel, 403_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send_not_found(frame, text = "")
      reply_text = "NOT_FOUND - #{text}"
      send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send_resource_locked(frame, text)
      reply_text = "RESOURCE_LOCKED - #{text}"
      send AMQP::Channel::Close.new(frame.channel, 405_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send_precondition_failed(frame, text)
      reply_text = "PRECONDITION_FAILED - #{text}"
      send AMQP::Channel::Close.new(frame.channel, 406_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send(frame : AMQP::Frame)
      @log.debug { "Send #{frame.inspect}"}
      @socket.write frame.to_slice
      unless frame.is_a?(AMQP::Basic::Deliver) || frame.is_a?(AMQP::HeaderFrame)
        @socket.flush
      end
      case frame
      when AMQP::Connection::CloseOk
        @log.info "Disconnected"
        @socket.close
        cleanup
      end
    rescue ex : IO::Error | Errno
      @log.info { "Lost connection, while sending (#{ex})" }
      cleanup
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect}" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    @acl_write_cache = Hash(String, Bool).new
    def can_write?(exchange_name)
      unless @acl_write_cache.has_key? exchange_name
        perm = @user.permissions[@vhost.name][:write]
        ok = perm != /^$/ && !!perm.match(exchange_name)
        @acl_write_cache[exchange_name] = ok
      end
      @acl_write_cache[exchange_name]
    end

    @acl_read_cache = Hash(String, Bool).new
    def can_read?(queue_name)
      unless @acl_read_cache.has_key? queue_name
        perm = @user.permissions[@vhost.name][:read]
        ok = perm != /^$/ && !!perm.match queue_name
        @acl_read_cache[queue_name] = ok
      end
      @acl_read_cache[queue_name]
    end

    @acl_config_cache = Hash(String, Bool).new
    def can_config?(name)
      unless @acl_config_cache.has_key? name
        perm = @user.permissions[@vhost.name][:config]
        ok = perm != /^$/ && !!perm.match name
        @acl_config_cache[name] = ok
      end
      @acl_config_cache[name]
    end
  end
end
