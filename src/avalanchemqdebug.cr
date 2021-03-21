require "amq-protocol"
require "./avalanchemq/version"
require "./avalanchemq/message.cr"
require "json"
require "option_parser"

alias AMQP = AMQ::Protocol

def ensure_data_directory!(dir)
  children = Dir.children(File.expend_path(dir))
  ok = true
  ok &&= children.includes? "users.json"
  ok &&= children.includes? "vhosts.json"
  unless ok
    puts "#{File.expand_path(dir)} not pointing to an AvalancheMQ data directory"
    exit 1
  end
end

def ensure_vhost_dir!(dir, name)
  json = File.open("#{dir}/vhosts.json") do |file|
    JSON.parse(file)
  end
  v = json.as_a.find { |row| row["name"].as_s == name }
  if v.nil?
    puts "vhost #{name} not found in vhosts.json file"
    exit 1
  end
  v["dir"].as_s
end

def show_content_for_sp(sp, opts)
  if sp.size != 20
    puts "invalid sp"
    exit 1
  end
  # 00000000040000047860
  segment = sp[..9].to_i32
  offset = sp[10..].to_i32
  segment_file = Path.new(opts["data_dir"], opts["vhost_dir"], "msgs.#{sp[..9]}")
  format = IO::ByteFormat::SystemEndian
  File.open(segment_file) do |io|
    io.seek(offset, IO::Seek::Set)
    ts = Int64.from_io io, format
    ex = AMQP::ShortString.from_io io, format
    rk = AMQP::ShortString.from_io io, format
    pr = AMQP::Properties.from_io io, format
    sz = UInt64.from_io io, format
    body = Bytes.new(sz)
    io.read(body)
    props = [] of String
    props << "  Content type:     #{pr.content_type}" if pr.content_type
    props << "  Content encoding: #{pr.content_encoding}" if pr.content_encoding
    props << "  Delivery mode:    #{pr.delivery_mode}" if pr.delivery_mode
    props << "  Priority:         #{pr.priority}" if pr.priority
    props << "  Correlation id:   #{pr.correlation_id}" if pr.correlation_id
    props << "  Reply to:         #{pr.reply_to}" if pr.reply_to
    props << "  Expiration:       #{pr.expiration}" if pr.expiration
    props << "  Message id:       #{pr.message_id}" if pr.message_id
    props << "  Timestamp:        #{pr.timestamp}" if pr.timestamp
    props << "  Type:             #{pr.type}" if pr.type
    props << "  User id:          #{pr.user_id}" if pr.user_id
    props << "  App id:           #{pr.app_id}" if pr.app_id
    props << "  Headers:          #{pr.headers}" if pr.headers
    print "Segment:     #{segment}
Offset:      #{offset}
Timestamp:   #{Time::Format.new("%F %X").format(Time.unix_ms(ts))}
Exchange:    #{ex}
Routing key: #{rk}
Size:        #{sz}
Properties
#{props.join('\n')}
#{String.new(body)}\n"
  end
end

def list_queues(data_dir, vhost_dir)
  Dir.children(Path.new(data_dir, vhost_dir)).each do |c|
    file_path = Path.new(data_dir, vhost_dir, c, ".queue")
    next unless File.exists? file_path
    name = File.read(file_path)
    puts "#{c} #{name}"
  end
end

options = {
  "data_dir" => ".",
  "vhost_name" => "/",
  "vhost_dir" => "42099b4af021e53fd8fd4e056c2568d7c2e3ffa8"
}
command = ""

OptionParser.parse do |parser|
  parser.on("-d dir", "--data-dir=dir", "Path to the AvalancheMQ data directory") do |v|
    options["data_dir"] = v
  end
  parser.on("-p vhost", "--vhost=vhost", "Which vhost") do |v|
    options["vhost_name"] = v
  end
  parser.on("list_queues", "") do
    command = "list_queues"
  end
  parser.on("show_sp", "Show message content for a SegmentPosition") do
    command = "show_sp"
  end
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit 0
  end
end

ensure_data_directory! options["data_dir"]
options["vhost_dir"] = ensure_vhost_dir! options["data_dir"], options["vhost_name"]

case command
when "show_sp"
  sp = ARGV.shift?
  exit 1 unless sp
  show_content_for_sp(sp, options)
when "list_queues"
  list_queues(options["data_dir"], options["vhost_dir"])
end
exit 0
