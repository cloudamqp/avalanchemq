require "./version"
require "../stdlib/*"
require "./config"
require "file"
require "systemd"
require "./reporter"

module AvalancheMQ
  class Launcher
    @tls_context : (OpenSSL::SSL::Context::Server|Nil)
    @lock : File
    @first_shutdown_attempt = true

    def initialize(@config : AvalancheMQ::Config)
      puts "AvalancheMQ #{AvalancheMQ::VERSION}"
      {% unless flag?(:release) %}
        puts "WARNING: Not built in release mode"
      {% end %}
      {% if flag?(:preview_mt) %}
        puts "Multithreading: #{ENV.fetch("CRYSTAL_WORKERS", "4")} threads"
      {% end %}
      puts "Pid: #{Process.pid}"
      puts "Data directory: #{@config.data_dir}"

      maximize_fd_limit
      @lock = acquire_lock

      @log = Logger.new(STDOUT, level: @config.log_level.not_nil!)
      AvalancheMQ::LogFormatter.use(@log)
      @amqp_server = AvalancheMQ::Server.new(@config.data_dir, @log.dup)
      @http_server = AvalancheMQ::HTTP::Server.new(@amqp_server, @log.dup)

      initiate_tls_context
      listen
      setup_signal_traps

      SystemD.notify("READY=1\n")
      GC.collect

      hold_lock(@lock)
    end

    def listen
      SystemD.listen_fds_with_names.each do |fd, name|
        protocol = systemd_socket_protocol(name)
        type = systemd_fd_type(fd)

        case {protocol, type}
        when {:amqp, :tcp}
          spawn @amqp_server.listen(TCPServer.new(fd: fd)), name: "AMQP server listener"
        when {:amqp, :unix}
          spawn @amqp_server.listen(UNIXServer.new(fd: fd)), name: "AMQP server listener"
        when {:http, :tcp}
          @http_server.bind(TCPServer.new(fd: fd))
        when {:http, :unix}
          @http_server.bind(UNIXServer.new(fd: fd))
        else
          # TODO: support resuming client connections
          # io = TCPSocket.new(fd: fd)
          # load_parameters_such_as_username_etc
          # Client.new(io, ...)
          puts "unexpected socket from systemd '#{name}' (#{fd})"
        end
      end

      tls = @tls_context
      @config.configured_tcp_listeners.each do |protocol, bind, port|
        case protocol
        when :amqp
          spawn @amqp_server.listen(bind, port), name: "AMQP listening on #{port}"
        when :amqps
          spawn @amqp_server.listen_tls(bind, port, tls.not_nil!), name: "AMQPS listening on #{port}"
        when :http
          @http_server.bind_tcp(bind, port)
        when :https
          @http_server.bind_tls(bind, port, tls.not_nil!)
        end
      end

      @config.configured_socket_listeners.each do |protocol, path|
        case protocol
        when :amqp
          spawn @amqp_server.listen_unix(path), name: "AMQP listening at #{path}"
        when :http
          @http_server.bind_unix(path)
        end
      end

      @http_server.bind_internal_unix
      spawn(name: "HTTP listener") do
        @http_server.not_nil!.listen
      end
    end

    def systemd_fd_type(fd)
      case
      when SystemD.is_tcp_listener?(fd) then :tcp
      when SystemD.is_unix_stream_listener?(fd) then :unix
      end
    end

    def systemd_socket_protocol(name)
      case name
      when @config.amqp_systemd_socket_name then :amqp
      when @config.http_systemd_socket_name then :http
      else
        raise "Unsupported #{name} socket type"
      end
    end

    def dump_debug_info
      STDOUT.puts System.resource_usage
      STDOUT.puts GC.prof_stats
      puts "Fibers:"
      Fiber.list { |f| puts f.inspect }
      AvalancheMQ::Reporter.report(@amqp_server)
      STDOUT.puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
      File.open(File.join(@amqp_server.data_dir, "string_pool.dump"), "w") do |f|
        STDOUT.puts "Dumping string pool to #{f.path}"
        AvalancheMQ::Reporter.dump_string_pool(f)
      end
      STDOUT.flush
    end

    def run_gc
      STDOUT.puts "Garbage collecting"
      STDOUT.flush
      GC.collect
    end

    def reload_server
      SystemD.notify("RELOADING=1\n")
      if @config.config_file.empty?
        @log.info { "No configuration file to reload" }
      else
        @log.info { "Reloading configuration file '#{@config.config_file}'" }
        @config.parse(@config.config_file)
        reload_log
        reload_tls_context
      end
      SystemD.notify("READY=1\n")
    end

    def shutdown_server
      if @first_shutdown_attempt
        @first_shutdown_attempt = false
        SystemD.notify("STOPPING=1\n")
        #@amqp_server.vhosts.each do |_, vh|
        #  SystemD.store_fds(vh.connections.map(&.fd), "vhost=#{vh.dir}")
        #end
        puts "Shutting down gracefully..."
        @amqp_server.close
        @http_server.try &.close
        @lock.close
        puts "Fibers: "
        Fiber.yield
        Fiber.list { |f| puts f.inspect }
        exit 0
      else
        puts "Fibers: "
        Fiber.list { |f| puts f.inspect }
        exit 1
      end
    end

    def setup_signal_traps
      Signal::USR1.trap { dump_debug_info }
      Signal::USR2.trap { run_gc }
      Signal::HUP.trap { reload_server }
      Signal::INT.trap { shutdown_server }
      Signal::TERM.trap { shutdown_server }
    end

    def initiate_tls_context
      return unless @config.tls_configured?
      context = OpenSSL::SSL::Context::Server.new
      context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
      @tls_context = context
      reload_tls_context
    end

    def reload_tls_context
      return unless tls = @tls_context
      case @config.tls_min_version
      when "1.0"
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                           OpenSSL::SSL::Options::NO_TLS_V1_1 |
                           OpenSSL::SSL::Options::NO_TLS_V1)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
      when "1.1"
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
      when "1.2", ""
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
      when "1.3"
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
      else
        @log.warn { "Unrecognized config value for tls_min_version: '#{@config.tls_min_version}'" }
      end
      tls.certificate_chain = @config.tls_cert_path
      tls.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
      tls.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
    end

    def reload_log
      new_level = @config.log_level || AvalancheMQ::Config::DEFAULT_LOG_LEVEL
      return if @log.level == new_level
      @log.info { "Log level changed from #{@log.level} to #{new_level}" }
      @log.level = @config.log_level.not_nil!
    end

    def maximize_fd_limit
      _, fd_limit_max = System.file_descriptor_limit
      System.file_descriptor_limit = fd_limit_max
      fd_limit_current, _ = System.file_descriptor_limit
      puts "FD limit: #{fd_limit_current}"
      if fd_limit_current < 1025
        puts "WARNING: The file descriptor limit is very low, consider raising it."
        puts "WARNING: You need one for each connection and two for each durable queue, and some more."
      end
    end

    # Make sure that only one instance is using the data directory
    # Can work as a poor mans cluster where the master nodes aquires
    # a file lock on a shared file system like NFS
    def acquire_lock : File
      Dir.mkdir_p @config.data_dir
      lock = File.open(File.join(@config.data_dir, ".lock"), "w+")
      lock.sync = true
      lock.read_buffering = false
      begin
        lock.flock_exclusive(blocking: false)
      rescue
        puts "INFO: Data directory locked by '#{lock.gets_to_end}'"
        puts "INFO: Waiting for file lock to be released"
        lock.flock_exclusive(blocking: true)
        puts "INFO: Lock aquired"
      end
      lock.truncate
      lock.print System.hostname
      lock.fsync
      lock
    end

    # write to the lock file to detect lost lock
    # See "Lost locks" in `man 2 fcntl`
    def hold_lock(lock)
      hostname = System.hostname.to_slice
      loop do
        sleep 30
        lock.write_at hostname, 0
      end
    rescue ex : IO::Error
      STDERR.puts ex.inspect
      abort "ERROR: Lost lock!"
    end
  end
end
