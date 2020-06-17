require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "option_parser"
require "file"
require "ini"

config_file = ""
config = AvalancheMQ::Config.instance

p = OptionParser.parse do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| config_file = v }
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
  parser.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: 5672)") do |v|
    config.amqp_port = v.to_i
  end
  parser.on("--amqps-port=PORT", "AMQPS port to listen on (default: -1)") do |v|
    config.amqps_port = v.to_i
  end
  parser.on("--http-port=PORT", "HTTP port to listen on (default: 15672)") do |v|
    config.http_port = v.to_i
  end
  parser.on("--https-port=PORT", "HTTPS port to listen on (default: -1)") do |v|
    config.https_port = v.to_i
  end
  parser.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
    config.unix_path = v
  end
  parser.on("--cert FILE", "TLS certificate (including chain)") { |v| config.cert_path = v }
  parser.on("--key FILE", "Private key for the TLS certificate") { |v| config.key_path = v }
  parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
    level = Log::Severity.parse?(v.to_s)
    config.log_level = level if level
  end
  parser.on("-d", "--debug", "Verbose logging") { config.log_level = Log::Severity::Debug }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

config.parse(config_file) unless config_file.empty?

if config.data_dir.empty?
  STDERR.puts "No data directory specified"
  STDERR.puts p
  exit 2
end

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"

puts "AvalancheMQ #{AvalancheMQ::VERSION}"
{% unless flag?(:release) %}
  puts "WARNING: Not built in release mode"
{% end %}
{% if flag?(:preview_mt) %}
  puts "Multithreading: #{ENV.fetch("CRYSTAL_WORKERS", "4")} threads"
{% end %}
puts "Pid: #{Process.pid}"
puts "Data directory: #{config.data_dir}"

# Maximize FD limit
_, fd_limit_max = System.file_descriptor_limit
System.file_descriptor_limit = fd_limit_max
fd_limit_current, _ = System.file_descriptor_limit
puts "FD limit: #{fd_limit_current}"
if fd_limit_current < 1025
  puts "WARNING: The file descriptor limit is very low, consider raising it."
  puts "WARNING: You need one for each connection and two for each durable queue, and some more."
end

# Make sure that only one instance is using the data directory
# Can work as a poor mans cluster where the master nodes aquires
# a file lock on a shared file system like NFS
Dir.mkdir_p config.data_dir
lock = File.open(File.join(config.data_dir, ".lock"), "w+")
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


backend = Log::IOBackend.new
backend.formatter = ->(entry : Log::Entry, io : IO) do
  io << entry
end

Log.builder.bind "*", config.log_level, backend
amqp_server = AvalancheMQ::Server.new(config.data_dir)

if config.amqp_port > 0
  spawn(name: "AMQP listening on #{config.amqp_port}") do
    amqp_server.not_nil!.listen(config.amqp_bind, config.amqp_port)
  end
end

if config.amqps_port > 0 && !config.cert_path.empty?
  spawn(name: "AMQPS listening on #{config.amqps_port}") do
    amqp_server.not_nil!.listen_tls(config.amqp_bind, config.amqps_port,
                                    config.cert_path,
                                    config.key_path || config.cert_path)
  end
end

unless config.unix_path.empty?
  spawn(name: "AMQP listening at #{config.unix_path}") do
    amqp_server.not_nil!.listen_unix(config.unix_path)
  end
end

if config.http_port > 0 || config.https_port > 0
  http_server = AvalancheMQ::HTTP::Server.new(amqp_server)
  if config.http_port > 0
    http_server.bind_tcp(config.http_bind, config.http_port)
  end
  if config.https_port > 0 && !config.cert_path.empty?
    http_server.bind_tls(config.http_bind, config.https_port,
                         config.cert_path,
                         config.key_path || config.cert_path)
  end
  spawn(name: "HTTP listener") do
    http_server.not_nil!.listen
  end
end

macro puts_size_capacity(obj, indent = 0)
  STDOUT << " " * {{ indent }}
  STDOUT << "{{ obj.name }}"
  STDOUT << " size="
  STDOUT << {{obj}}.size
  STDOUT << " capacity="
  STDOUT << {{obj}}.capacity
  STDOUT << '\n'
end

def report(s)
  puts "Flow=#{s.flow?}"
  puts_size_capacity s.@connections
  s.connections.each do |c|
    puts "  #{c.name}"
    puts_size_capacity c.@channels, 4
    c.channels.each_value do |ch|
      puts "    #{ch.id} prefetch=#{ch.prefetch_size}"
      puts_size_capacity ch.@unacked, 6
      puts_size_capacity ch.@consumers, 6
      puts_size_capacity ch.@visited, 6
      puts_size_capacity ch.@found_queues, 6
    end
  end
  puts_size_capacity s.@users
  puts_size_capacity s.@vhosts
  s.vhosts.each do |_, vh|
    puts "VHost #{vh.name}"
    puts_size_capacity vh.@awaiting_confirm, 4
    puts_size_capacity vh.@exchanges, 4
    puts_size_capacity vh.@queues, 4
    puts_size_capacity vh.@zero_references, 4
    puts_size_capacity vh.@sp_counter, 4
    puts_size_capacity vh.@referenced_segments, 4
    vh.queues.each do |_, q|
      puts "    #{q.name} #{q.durable ? "durable" : ""} args=#{q.arguments}"
      puts_size_capacity q.@consumers, 6
      puts_size_capacity q.@ready, 6
      puts_size_capacity q.@unacked, 6
      puts_size_capacity q.@requeued, 6
      puts_size_capacity q.@deliveries, 6
    end
  end
end

def dump_string_pool(io)
  pool = AMQ::Protocol::ShortString::POOL
  io.puts "# size=#{pool.size} capacity=#{pool.@capacity}"
  pool.@capacity.times do |i|
    str = pool.@values[i]
    next if str.empty?
    io.puts str
  end
end

Signal::USR1.trap do
  STDOUT.puts System.resource_usage
  STDOUT.puts GC.prof_stats
  report(amqp_server)
  STDOUT.puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  File.open(File.join(amqp_server.data_dir, "string_pool.dump"), "w") do |f|
    STDOUT.puts "Dumping string pool to #{f.path}"
    dump_string_pool(f)
  end
  STDOUT.flush
end

Signal::USR2.trap do
  STDOUT.puts "Garbage collecting"
  STDOUT.flush
  GC.collect
end

Signal::HUP.trap do
  if config_file.empty?
    puts "No configuration file to reload"
  else
    puts "Reloading configuration file '#{config_file}'"
    config.parse(config_file)
  end
  STDOUT.flush
end

shutdown = ->(_s : Signal) do
  puts "Shutting down gracefully..."
  puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  puts System.resource_usage
  puts GC.prof_stats
  http_server.try &.close
  amqp_server.close
  Fiber.yield
  puts "Fibers: "
  Fiber.list { |f| puts f.inspect }
  lock.close
  exit 0
end
Signal::INT.trap &shutdown
Signal::TERM.trap &shutdown
if SystemD.notify_ready > 0
  log.info "Ready (Notified SystemD)"
end
GC.collect

# write to the lock file to detect lost lock
# See "Lost locks" in `man 2 fcntl`
begin
  hostname = System.hostname.to_slice
  loop do
    sleep 30
    lock.write_at hostname, 0
  end
rescue ex : IO::Error
  STDERR.puts ex.inspect
  abort "ERROR: Lost lock!"
end
