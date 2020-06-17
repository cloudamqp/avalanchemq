module AvalancheMQ
  class NetworkClient
    def self.start(socket, remote_address, local_address, vhosts, users)
      socket.read_timeout = 15
      confirm_header(socket) || return
      start_ok = start(socket)
      creds = credentials(start_ok)
      user = authenticate(socket, users, creds[:username], creds[:password], start_ok) || return
      tune_ok = tune(socket)
      if vhost = open(socket, vhosts, user)
        self.new(socket, remote_address, local_address, vhost, user, tune_ok, start_ok)
      else
        nil
      end
    rescue ex : Socket::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      Log.warn { "#{(ex.cause || ex).inspect} while #{remote_address} tried to establish connection" }
      begin
        socket.try &.close
      rescue
      end
      nil
    rescue ex
      Log.error(exception: ex) { "Error while #{remote_address} tried to establish connection #{ex.inspect}" }
      begin
        socket.try &.close
      rescue
      end
      nil
    ensure
      timeout =
        if t = tune_ok
          if t.heartbeat > 0
            t.heartbeat + 1
          end
        end
      socket.read_timeout = timeout
    end

    def self.confirm_header(socket)
      proto = uninitialized UInt8[8]
      socket.read(proto.to_slice)
      return true if proto == AMQP::PROTOCOL_START_0_9_1 ||
                     proto == AMQP::PROTOCOL_START_0_9
      Log.info { "Client #{socket.remote_address} sent bad protocol start header, closing: '#{proto}'" }
      socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
      socket.flush
      socket.close
      false
    end

    def self.start(socket)
      start = AMQP::Frame::Connection::Start.new
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
    end

    def self.credentials(start_ok)
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        { username: resp[1...i], password: resp[(i + 1)..-1] }
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.bytesize.to_u32)
        { username: tbl["LOGIN"].as(String), password: tbl["PASSWORD"].as(String) }
      else raise "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end
    end

    def self.authenticate(socket, users, username, password, start_ok)
      user = users[username]?
      return user if user && user.password && user.password.not_nil!.verify(password)

      if user.nil?
        Log.warn { "User \"#{username}\" not found" }
      else
        Log.warn { "Authentication failure for user \"#{username}\"" }
      end
      props = start_ok.client_properties
      capabilities = props["capabilities"]?.try &.as(AMQP::Table)
      if capabilities && capabilities["authentication_failure_close"]?.try &.as(Bool)
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED",
          start_ok.class_id,
          start_ok.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket)
      else
        socket.close
      end
      nil
    end

    def self.tune(socket)
      socket.write_bytes AMQP::Frame::Connection::Tune.new(
        channel_max: Config.instance.channel_max,
        frame_max: Config.instance.frame_max,
        heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::TuneOk) }
    end

    def self.open(socket, vhosts, user)
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return vhost
        else
          Log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{open.vhost}\"" }
          reply_text = "NOT_ALLOWED - '#{user.name}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket)
        end
      else
        Log.warn { "VHost \"#{open.vhost}\" not found" }
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket)
      end
      nil
    end

    def self.close_on_ok(socket)
      loop do
        AMQP::Frame.from_io(socket, IO::ByteFormat::NetworkEndian) do |frame|
          if frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk)
            return
          else
            Log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            if frame.is_a?(AMQP::Frame::Body)
              Log.debug { "Skipping body" }
              frame.body.skip(frame.body_size)
            end
          end
        end
      end
    rescue IO::EOFError
      Log.debug { "Client closed socket without sending CloseOk" }
    rescue ex : IO::Error | AMQP::Error::FrameDecode
      Log.debug(exception: ex) { "#{ex.inspect} when waiting for CloseOk" }
    ensure
      socket.close
    end
  end
end
