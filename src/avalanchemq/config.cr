require "log"

module AvalancheMQ
  class Config
    property data_dir = ""
    property log_level : Log::Severity = Log::Severity::Info
    property amqp_bind = "0.0.0.0"
    property amqp_port = 5672
    property amqps_port = -1
    property unix_path = ""
    property cert_path = ""
    property key_path = ""
    property http_bind = "0.0.0.0"
    property http_port = 15672
    property https_port = -1
    property heartbeat = 0_u16 # second
    property segment_size : Int32 = 32 * 1024**2 # byte
    property gc_segments_interval = 60 # second
    property queue_max_acks = 2_000_000 # number of message
    property stats_interval = 5000 # millisecond
    property stats_log_size = 120 # 10 mins at 5s interval
    property set_timestamp = false # in message headers when receive
    property file_buffer_size = 131_072 # byte
    property socket_buffer_size = 16384 # byte

    @@instance : Config = self.new

    def self.instance
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
        end
      end
    end

    private def parse_main(settings)
      settings["data_dir"]?.try { |v| @data_dir = v }
      settings["log_level"]?.try { |v| @log_level = Log::Severity.parse(v) }
      settings["stats_interval"]?.try { |v| @stats_interval = v.to_i32 }
      settings["stats_log_size"]?.try { |v| @stats_log_size = v.to_i32 }
      settings["gc_segments_interval"]?.try { |v| @gc_segments_interval = v.to_i32 }
      settings["segment_size"]?.try { |v| @segment_size = v.to_i32 }
      settings["queue_max_acks"]?.try { |v| @queue_max_acks = v.to_i32 }
      settings["set_timestamp"]?.try { |v| @set_timestamp = true?(v) }
      settings["file_buffer_size"]?.try { |v| @file_buffer_size = v.to_i32 }
      settings["socket_buffer_size"]?.try { |v| @socket_buffer_size = v.to_i32 }
      settings["tls_cert"]?.try { |v| @cert_path = v }
      settings["tls_key"]?.try { |v| @key_path = v }
    end

    private def parse_amqp(settings)
      settings["bind"]?.try { |v| @amqp_bind = v }
      settings["port"]?.try { |v| @amqp_port = v.to_i32 }
      settings["tls_port"]?.try { |v| @amqps_port = v.to_i32 }
      settings["tls_cert"]?.try { |v| @cert_path = v } # backward compatibility
      settings["tls_key"]?.try { |v| @key_path = v } # backward compatibility
      settings["unix_path"]?.try { |v| @unix_path = v }
      settings["heartbeat"]?.try { |v| @heartbeat = v.to_u16 }
    end

    private def parse_mgmt(settings)
      settings["bind"]?.try { |v| @http_bind = v }
      settings["port"]?.try { |v| @http_port = v.to_i32 }
      settings["tls_port"]?.try { |v| @https_port = v.to_i32 }
      settings["tls_cert"]?.try { |v| @cert_path = v } # backward compatibility
      settings["tls_key"]?.try { |v| @key_path = v } # backward compatibility
    end

    private def true?(str : String?)
      { "true", "yes", "y", "1" }.includes? str
    end
  end
end
