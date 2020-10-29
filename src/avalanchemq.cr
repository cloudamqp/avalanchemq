require "file"
require "./avalanchemq/config"
require "./avalanchemq/server_cli"
require "./avalanchemq/launcher"

config_dir = ENV.fetch("ConfigurationDirectory", "/etc/avalanchemq")
config_file = File.join(config_dir, "avalanchemq.ini")
config_file = "" unless File.exists?(config_file)

config = AvalancheMQ::Config.instance

AvalancheMQ::ServerCLI.new(config, config_file).parse

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"

AvalancheMQ::Launcher.new(config) # will block
