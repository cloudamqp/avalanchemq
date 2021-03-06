require "http/server"
require "http-protection"
require "json"
require "logger"
require "router"
require "./handler/*"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module AvalancheMQ
  module HTTP
    INTERNAL_UNIX_SOCKET = "/tmp/avalanchemq/http.sock"

    class Server
      def initialize(@amqp_server : AvalancheMQ::Server, @log : Logger)
        @log.progname = "httpserver"
        handlers = [
          ::HTTP::Protection::StrictTransport.new,
          ::HTTP::Protection::FrameOptions.new,
          ::HTTP::CompressHandler.new,
          AMQPWebsocket.new(@amqp_server),
          ApiDefaultsHandler.new,
          ApiErrorHandler.new(@log.dup),
          StaticController.new,
          BasicAuthHandler.new(@amqp_server.users, @log.dup),
          MainController.new(@amqp_server, @log.dup).route_handler,
          DefinitionsController.new(@amqp_server, @log.dup).route_handler,
          ConnectionsController.new(@amqp_server, @log.dup).route_handler,
          ChannelsController.new(@amqp_server, @log.dup).route_handler,
          ConsumersController.new(@amqp_server, @log.dup).route_handler,
          ExchangesController.new(@amqp_server, @log.dup).route_handler,
          QueuesController.new(@amqp_server, @log.dup).route_handler,
          BindingsController.new(@amqp_server, @log.dup).route_handler,
          VHostsController.new(@amqp_server, @log.dup).route_handler,
          UsersController.new(@amqp_server, @log.dup).route_handler,
          PermissionsController.new(@amqp_server, @log.dup).route_handler,
          ParametersController.new(@amqp_server, @log.dup).route_handler,
          NodesController.new(@amqp_server, @log.dup).route_handler,
          PrometheusController.new(@amqp_server, @log.dup).route_handler,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new) if @log.level == Logger::DEBUG
        @http = ::HTTP::Server.new(handlers)
      end

      def bind(socket)
        addr = @http.bind(socket)
        @log.info { "Bound to #{addr}" }
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        @log.info { "Bound to #{addr}" }
      end

      def bind_tls(address, port, ctx)
        addr = @http.bind_tls address, port, ctx
        @log.info { "Bound on #{addr}" }
      end

      def bind_unix(path)
        addr = @http.bind_unix path
        @log.info { "Bound to #{addr}" }
      end

      def bind_internal_unix
        if File.exists? INTERNAL_UNIX_SOCKET
          File.delete INTERNAL_UNIX_SOCKET
        else
          FileUtils.mkdir_p(File.dirname(INTERNAL_UNIX_SOCKET))
        end
        addr = @http.bind_unix INTERNAL_UNIX_SOCKET
        File.chmod(INTERNAL_UNIX_SOCKET, 0o770)
        @log.info { "Bound to #{addr}" }
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
      end

      def closed?
        @http.closed?
      end

      class PayloadTooLarge < Exception; end

      class NotFoundError < Exception; end

      class ExpectedBodyError < ArgumentError; end

      class UnknownContentType < Exception; end
    end
  end
end
