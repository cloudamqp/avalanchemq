require "logger"

module AvalancheMQ
  class Config
    DEFAULT_LOG_LEVEL = Logger::INFO

    property data_dir : String = ENV.fetch("STATE_DIRECTORY", "")
    property config_file = ""
    property log_level : Logger::Severity = DEFAULT_LOG_LEVEL
    property amqp_bind = "127.0.0.1"
    property amqp_port = 5672
    property amqps_port = -1
    property unix_path = ""
    property unix_proxy_protocol = true # PROXY protocol on unix domain socket connections
    property tcp_proxy_protocol = false  # PROXY protocol on amqp tcp connections
    property tls_cert_path = ""
    property tls_key_path = ""
    property tls_ciphers = ""
    property tls_min_version = ""
    property http_bind = "127.0.0.1"
    property http_port = 15672
    property https_port = -1
    property http_unix_path = ""
    property http_systemd_socket_name = "avalanchemq-http.socket"
    property amqp_systemd_socket_name = "avalanchemq-amqp.socket"
    property heartbeat = 0_u16                   # second
    property frame_max = 1048576_u32             # bytes
    property channel_max = 2048_u16              # number
    property segment_size : Int32 = 1024**3      # byte
    property gc_segments_interval = 60           # second
    property queue_max_acks = 2_000_000          # number of message
    property stats_interval = 5000               # millisecond
    property stats_log_size = 120                # 10 mins at 5s interval
    property set_timestamp = false               # in message headers when receive
    property file_buffer_size = 16384            # byte
    property socket_buffer_size = 16384          # byte
    property tcp_nodelay = false                 # bool

    @@instance : Config = self.new

    def self.instance : AvalancheMQ::Config
      @@instance
    end

    def parse(file)
      return if file.empty?
      abort "Config could not be found" unless File.file?(file)
      ini = INI.parse(File.read(file))
      ini.each do |section, settings|
        case section
        when "main"
          parse_main(settings)
        when "amqp"
          parse_amqp(settings)
        when "mgmt", "http"
          parse_mgmt(settings)
        else
          raise "Unrecognized config section: #{section}"
        end
      end
    end

    def configured_tcp_listeners
      if (amqps_port > 0 || https_port > 0) && !tls_configured?
        STDERR.puts "WARNING: Certificate for TLS not configured, ignoring AMQPS/HTTPS listeners"
      end

      listeners = [] of { Symbol, String, Int32 }
      listeners << { :amqp, @amqp_bind, @amqp_port } if @amqp_port > 0
      listeners << { :amqps, @amqp_bind, @amqps_port } if tls_configured? && @amqps_port > 0
      listeners << { :http, @http_bind, @http_port } if @http_port > 0
      listeners << { :https, @http_bind, @https_port } if tls_configured? && @https_port > 0
      listeners
    end

    def configured_socket_listeners
      listeners = [] of { Symbol, String }
      listeners << { :amqp, @unix_path } unless @unix_path.empty?
      listeners << { :http, @http_unix_path } unless @http_unix_path.empty?
      listeners
    end

    def tls_configured?
      !@tls_cert_path.empty?
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_main(settings)
      settings.each do |config, v|
        case config
        when "data_dir"             then @data_dir = v
        when "log_level"            then @log_level = Logger::Severity.parse(v)
        when "stats_interval"       then @stats_interval = v.to_i32
        when "stats_log_size"       then @stats_log_size = v.to_i32
        when "gc_segments_interval" then @gc_segments_interval = v.to_i32
        when "segment_size"         then @segment_size = v.to_i32
        when "queue_max_acks"       then @queue_max_acks = v.to_i32
        when "set_timestamp"        then @set_timestamp = true?(v)
        when "file_buffer_size"     then @file_buffer_size = v.to_i32
        when "socket_buffer_size"   then @socket_buffer_size = v.to_i32
        when "tcp_nodelay"          then @tcp_nodelay = true?(v)
        when "tls_cert"             then @tls_cert_path = v
        when "tls_key"              then @tls_key_path = v
        when "tls_ciphers"          then @tls_ciphers = v
        when "tls_min_version"      then @tls_min_version = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'main/#{config}'"
        end
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_amqp(settings)
      settings.each do |config, v|
        case config
        when "bind"        then @amqp_bind = v
        when "port"        then @amqp_port = v.to_i32
        when "tls_port"    then @amqps_port = v.to_i32
        when "tls_cert"    then @tls_cert_path = v # backward compatibility
        when "tls_key"     then @tls_key_path = v  # backward compatibility
        when "unix_path"   then @unix_path = v
        when "heartbeat"   then @heartbeat = v.to_u16
        when "frame_max"   then @frame_max = v.to_u32
        when "channel_max" then @channel_max = v.to_u16
        when "systemd_socket_name" then @amqp_systemd_socket_name = v
        when "unix_proxy_protocol" then @unix_proxy_protocol = true?(v)
        when "tcp_proxy_protocol"  then @tcp_proxy_protocol = true?(v)
        else
          STDERR.puts "WARNING: Unrecognized configuration 'amqp/#{config}'"
        end
      end
    end

    private def parse_mgmt(settings)
      settings.each do |config, v|
        case config
        when "bind"      then @http_bind = v
        when "port"      then @http_port = v.to_i32
        when "tls_port"  then @https_port = v.to_i32
        when "tls_cert"  then @tls_cert_path = v # backward compatibility
        when "tls_key"   then @tls_key_path = v  # backward compatibility
        when "unix_path" then @http_unix_path = v
        when "systemd_socket_name" then @http_systemd_socket_name = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'mgmt/#{config}'"
        end
      end
    end

    private def true?(str : String?)
      {"true", "yes", "y", "1"}.includes? str
    end
  end
end
