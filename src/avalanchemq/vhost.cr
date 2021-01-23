require "benchmark"
require "json"
require "logger"
require "../stdlib/*"
require "./segment_position"
require "./vhost/*"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/shovel_store"
require "./federation/upstream_store"
require "./sortable_json"
require "./exchange"
require "digest/sha1"
require "./queue"
require "./mfile"
require "./schema"

module AvalancheMQ
  class VHost
    include SortableJSON

    getter name, exchanges, queues, log, data_dir, policies, parameters,
      log, shovels, direct_reply_channels, upstreams, default_user,
      connections, dir
    property? flow = true
    property? dirty = false
    getter? closed = false

    @exchanges = Hash(String, Exchange).new
    @queues = Hash(String, Queue).new
    @save = Channel(AMQP::Frame).new(32)
    @write_lock = Mutex.new(:unchecked)
    @wfile : MFile
    @log : Logger
    @direct_reply_channels = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    @fsync = false
    @connections = Array(Client).new(512)
    @segments : Hash(UInt32, MFile)
    EXCHANGE_TYPES = %w(direct fanout topic headers
      x-federation-upstream x-delayed-message
      x-consistent-hash)

    def initialize(@name : String, @server_data_dir : String,
                   @log : Logger, @default_user : User, @events : Server::Event)
      @log.progname = "vhost=#{@name}"
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir, "tmp")
      File.write(File.join(@data_dir, ".vhost"), @name)
      @segments = load_segments_on_disk
      @wfile = @segments.last_value
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
      spawn save!, name: "VHost/#{@name}#save!"
      spawn gc_segments_loop, name: "VHost/#{@name}#gc_segments_loop"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << ">"
    end

    @awaiting_confirm_lock = Mutex.new(:unchecked)
    @awaiting_confirm = Set(Client::Channel).new

    def waiting4confirm(channel)
      @awaiting_confirm_lock.synchronize do
        @awaiting_confirm.add channel
        unless @fsync
          @fsync = true
          spawn(name: "VHost/#{@name}#fsync") { fsync }
        end
      end
    end

    @queues_to_fsync_lock = Mutex.new(:unchecked)
    @queues_to_fsync = Set(DurableQueue).new

    def fsync
      return unless @fsync
      @log.debug { "fsync segment file" }
      @wfile.fsync
      @queues_to_fsync_lock.synchronize do
        @log.debug { "fsyncing #{@queues_to_fsync.size} queues" }
        @queues_to_fsync.each &.fsync_enq
        @queues_to_fsync.clear
      end
      @awaiting_confirm_lock.synchronize do
        @log.debug { "send confirm to #{@awaiting_confirm.size} channels" }
        @awaiting_confirm.each do |ch|
          ch.confirm_ack(multiple: true)
        end
        @awaiting_confirm.clear
        @fsync = false
      end
    end

    # Queue#publish can raise RejectPublish which should trigger a Nack. All other confirm scenarios
    # should be Acks, apart from Exceptions.
    # As long as at least one queue reject the publish due to overflow a Nack should be sent,
    # even if other queues accepts the message. Behaviour confirmed with RabbitMQ.
    # True if it also succesfully wrote to one or more queues
    # False if no queue was able to receive the message because they're
    # closed
    def publish(msg : Message, immediate = false,
                visited = Set(Exchange).new, found_queues = Set(Queue).new, confirm = false) : Bool
      ex = @exchanges[msg.exchange_name]? || return false
      ex.publish_in_count += 1
      properties = msg.properties
      headers = properties.headers
      find_all_queues(ex, msg.routing_key, headers, visited, found_queues)
      headers.try(&.delete("BCC"))
      @log.debug { "publish queues#found=#{found_queues.size}" }
      if found_queues.empty?
        ex.unroutable_count += 1
        return false
      end
      return false if immediate && !found_queues.any? { |q| q.immediate_delivery? }
      sp = write_to_disk(msg, ex.persistent?)
      flush = properties.delivery_mode == 2_u8
      ok = 0
      found_queues.each do |q|
        if q.publish(sp, flush)
          ex.publish_out_count += 1
          if confirm && q.is_a?(DurableQueue) && flush
            @queues_to_fsync_lock.synchronize do
              @queues_to_fsync << q
            end
          end
          ok += 1
        end
      end
      ok > 0
    ensure
      visited.clear
      found_queues.clear
    end

    private def find_all_queues(ex : Exchange, routing_key : String,
                                headers : AMQP::Table?,
                                visited : Set(Exchange),
                                queues : Set(Queue)) : Nil
      ex.queue_matches(routing_key, headers) do |q|
        queues.add? q
      end

      ex.exchange_matches(routing_key, headers) do |e2e|
        visited.add(ex)
        if visited.add?(e2e)
          find_all_queues(e2e, routing_key, headers, visited, queues)
        end
      end

      if cc = headers.try(&.fetch("CC", nil))
        if cc = cc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          cc.each do |rk|
            if rk = rk.as?(String)
              find_all_queues(ex, rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("CC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("CC header not a string array")
        end
      end

      if bcc = headers.try(&.fetch("BCC", nil))
        if bcc = bcc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          hdrs.delete "BCC"
          bcc.each do |rk|
            if rk = rk.as?(String)
              find_all_queues(ex, rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("BCC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("BCC header not a string array")
        end
      end

      if queues.empty? && ex.alternate_exchange
        if ae = @exchanges[ex.alternate_exchange]?
          visited.add(ex)
          if visited.add?(ae)
            find_all_queues(ae, routing_key, headers, visited, queues)
          end
        end
      end
    end

    private def write_to_disk(msg, store_offset = false) : SegmentPosition
      @write_lock.synchronize do
        wfile = @wfile
        # Set x-offset before we have a SP so that the Message#bytesize is correct
        if store_offset
          headers = msg.properties.headers || AMQP::Table.new
          headers["x-offset"] = 0_i64
          msg.properties.headers = headers
        end
        if wfile.capacity < wfile.size + msg.bytesize
          wfile = open_new_segment(msg.bytesize)
        end
        pos = wfile.seek(0, IO::Seek::End)
        sp = SegmentPosition.make(@segments.last_key, pos.to_u32, msg)
        if store_offset
          msg.properties.headers.not_nil!["x-offset"] = sp.to_i64
        end
        @log.debug { "Writing message: exchange=#{msg.exchange_name} routing_key=#{msg.routing_key} \
                      size=#{msg.bytesize} sp=#{sp}" }
        wfile.write_bytes msg.timestamp
        wfile.write_bytes AMQP::ShortString.new(msg.exchange_name)
        wfile.write_bytes AMQP::ShortString.new(msg.routing_key)
        wfile.write_bytes msg.properties
        wfile.write_bytes msg.size # bodysize
        copied = IO.copy(msg.body_io, wfile, msg.size)
        if copied != msg.size
          raise IO::Error.new("Could only write #{copied} of #{msg.size} bytes to message store")
        end
        sp
      end
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      fsync
      segments = @segments
      next_id = segments.empty? ? 1_u32 : segments.last_key + 1
      @log.debug { "Opening message store segment #{next_id}" }
      filename = "msgs.#{next_id.to_s.rjust(10, '0')}"
      path = File.join(@data_dir, filename)
      capacity = Config.instance.segment_size + next_msg_size
      wfile = MFile.new(path, capacity)
      SchemaVersion.prefix(wfile, :message)
      @wfile = segments[next_id] = wfile
    end

    def segment_file(id : UInt32) : MFile
      @segments[id]
    end

    def details_tuple
      {
        name:          @name,
        dir:           @dir,
        tracing:       false,
        cluster_state: NamedTuple.new,
      }
    end

    def message_details
      ready = unacked = 0
      ack = confirm = deliver = get = get_no_ack = publish = redeliver = return_unroutable = 0
      @queues.each_value do |q|
        ready += q.message_count
        unacked += q.unacked_count
        ack += q.ack_count
        confirm += q.confirm_count
        deliver += q.deliver_count
        get += q.get_count
        get_no_ack += q.get_no_ack_count
        publish += q.publish_count
        redeliver += q.redeliver_count
        return_unroutable += q.return_unroutable_count
      end
      {
        messages:                ready + unacked,
        messages_unacknowledged: unacked,
        messages_ready:          ready,
        message_stats:           {
          ack:               ack,
          confirm:           confirm,
          deliver:           deliver,
          get:               get,
          get_no_ack:        get_no_ack,
          publish:           publish,
          redeliver:         redeliver,
          return_unroutable: return_unroutable,
        },
      }
    end

    def declare_queue(name, durable, auto_delete, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, durable, false,
        auto_delete, false, arguments)
      @log.info { "queue=#{name} (durable: #{durable}, auto_delete=#{auto_delete}, arguments: #{arguments}) Created" }
    end

    def delete_queue(name)
      apply AMQP::Frame::Queue::Delete.new(0_u16, 0_u16, name, false, false, false)
    end

    def declare_exchange(name, type, durable, auto_delete, internal = false,
                         arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, name, type, false, durable,
        auto_delete, internal, false, arguments)
      @log.info { "exchange=#{name} Created" }
    end

    def delete_exchange(name)
      apply AMQP::Frame::Exchange::Delete.new(0_u16, 0_u16, name, false, false)
    end

    def bind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    def bind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    def unbind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Unbind.new(0_u16, 0_u16, destination, source,
        routing_key, arguments)
    end

    def unbind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Unbind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    @apply_count = 0_u64

    # ameba:disable Metrics/CyclomaticComplexity
    def apply(f, loading = false) : Bool
      Fiber.yield if (@apply_count += 1) % 128 == 0
      case f
      when AMQP::Frame::Exchange::Declare
        return false if @exchanges.has_key? f.exchange_name
        e = @exchanges[f.exchange_name] =
          make_exchange(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments.to_h)
        apply_policies([e] of Exchange) unless loading
      when AMQP::Frame::Exchange::Delete
        if x = @exchanges.delete f.exchange_name
          @exchanges.each_value do |ex|
            ex.exchange_bindings.each do |binding_args, destinations|
              ex.unbind(x, *binding_args) if destinations.includes?(x)
            end
          end
          x.delete
        else
          return false
        end
      when AMQP::Frame::Exchange::Bind
        source = @exchanges[f.source]? || return false
        x = @exchanges[f.destination]? || return false
        source.bind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Exchange::Unbind
        source = @exchanges[f.source]? || return false
        x = @exchanges[f.destination]? || return false
        source.unbind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Declare
        return false if @queues.has_key? f.queue_name
        q = @queues[f.queue_name] = QueueFactory.make(self, f)
        apply_policies([q] of Queue) unless loading
        @events.send(EventType::QueueDeclared) unless loading
      when AMQP::Frame::Queue::Delete
        if q = @queues.delete(f.queue_name)
          @exchanges.each_value do |ex|
            ex.queue_bindings.each do |binding_args, destinations|
              ex.unbind(q, *binding_args) if destinations.includes?(q)
            end
          end
          @events.send(EventType::QueueDeleted)
          q.delete
        else
          return false
        end
      when AMQP::Frame::Queue::Bind
        x = @exchanges[f.exchange_name]? || return false
        q = @queues[f.queue_name]? || return false
        x.bind(q, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Unbind
        x = @exchanges[f.exchange_name]? || return false
        q = @queues[f.queue_name]? || return false
        x.unbind(q, f.routing_key, f.arguments.to_h)
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
      @save.send f unless loading
      true
    end

    def add_policy(name : String, pattern : Regex, apply_to : Policy::Target,
                   definition : Hash(String, JSON::Any), priority : Int8)
      add_policy(Policy.new(name, @name, pattern, apply_to, definition, priority))
      @log.info { "Policy=#{name} Created" }
    end

    def add_policy(p : Policy)
      @policies.delete(p.name)
      @policies.create(p)
      spawn apply_policies, name: "ApplyPolicies (after add) #{@name}"
    end

    def delete_policy(name)
      @policies.delete(name)
      spawn apply_policies, name: "ApplyPolicies (after delete) #{@name}"
      @log.info { "Policy=#{name} Deleted" }
    end

    def add_connection(client : Client)
      @connections << client
      client.on_close do |c|
        @connections.delete c
      end
    end

    SHOVEL                  = "shovel"
    FEDERATION_UPSTREAM     = "federation-upstream"
    FEDERATION_UPSTREAM_SET = "federation-upstream-set"

    def add_parameter(p : Parameter)
      @log.debug { "Add parameter #{p.name}" }
      @parameters.delete(p.name)
      @parameters.create(p)
      apply_parameters(p)
      spawn apply_policies, name: "ApplyPolicies (add parameter) #{@name}"
    end

    def delete_parameter(component_name, parameter_name)
      @log.debug { "Delete parameter #{parameter_name}" }
      @parameters.delete({component_name, parameter_name})
      case component_name
      when SHOVEL
        @shovels.not_nil!.delete(parameter_name)
      when FEDERATION_UPSTREAM
        @upstreams.not_nil!.delete_upstream(parameter_name)
      when FEDERATION_UPSTREAM_SET
        @upstreams.not_nil!.delete_upstream_set(parameter_name)
      else
        @log.warn { "No action when deleting parameter #{component_name}" }
      end
    end

    def stop_shovels
      @shovels.not_nil!.each_value &.terminate
    end

    def stop_upstream_links
      @upstreams.not_nil!.stop_all
    end

    def close(seamless_restart = false, reason = "Broker shutdown")
      @closed = true
      stop_shovels
      Fiber.yield
      stop_upstream_links
      Fiber.yield
      if seamless_restart
        # TODO: Handover FDs to systemd
        SystemD.store_fds(@connections.map &.fd, "vhost=#{@name}")
      else
        @log.debug "Closing connections"
        @connections.each &.close(reason)
        # wait up to 10s for clients to gracefully close
        100.times do
          break if @connections.empty?
          sleep 0.1
        end
        # then force close the remaining (close tcp socket)
        @connections.each &.force_close
        Fiber.yield # yield so that Client read_loops can shutdown
      end
      @write_lock.synchronize do
        @queues.each_value &.close
        @segments.each_value &.close
      end
      @save.close
      Fiber.yield
      compact!(include_transient: seamless_restart)
    end

    def delete
      close(reason: "VHost deleted")
      Fiber.yield
      FileUtils.rm_rf @data_dir
    end

    def consumers
      @connections.flat_map { |conn| conn.channels.each_value.flat_map &.consumers }
    end

    private def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      itr = if resources
              resources.each
            else
              @queues.each_value.chain(@exchanges.each_value)
            end
      sorted_policies = @policies.values.sort_by!(&.priority).reverse
      itr.each do |r|
        if match = sorted_policies.find { |p| p.match?(r) }
          r.apply_policy(match)
        else
          r.clear_policy
        end
      end
    rescue ex : TypeCastError
      @log.warn { "Invalid policy. #{ex.message}" }
    end

    private def apply_parameters(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        case p.component_name
        when SHOVEL
          @shovels.not_nil!.create(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM
          @upstreams.not_nil!.create_upstream(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM_SET
          @upstreams.not_nil!.create_upstream_set(p.parameter_name, p.value)
        else
          @log.warn { "No action when applying parameter #{p.component_name}" }
        end
      end
    end

    private def load!
      load_definitions!
      spawn(name: "Load parameters") do
        sleep 0.05
        apply_parameters
        apply_policies
      end
    end

    private def load_definitions!
      File.open(File.join(@data_dir, "definitions.amqp"), "r") do |io|
        io.buffer_size = Config.instance.file_buffer_size
        io.advise(File::Advice::Sequential)
        SchemaVersion.verify(io, :definition)
        loop do
          begin
            AMQP::Frame.from_io(io, IO::ByteFormat::SystemEndian) do |frame|
              apply frame, loading: true
            end
          rescue ex : IO::EOFError
            break
          end
        end
      end
    rescue IO::Error
      load_default_definitions
      compact!
    end

    private def load_default_definitions
      @log.info "Loading default definitions"
      @exchanges[""] = DefaultExchange.new(self, "", true, false, false)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", true, false, false)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", true, false, false)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", true, false, false)
      @exchanges["amq.headers"] = HeadersExchange.new(self, "amq.headers", true, false, false)
      @exchanges["amq.match"] = HeadersExchange.new(self, "amq.match", true, false, false)
    end

    private def compact!(include_transient = false)
      @log.info "Compacting definitions"
      tmp_path = File.join(@data_dir, "definitions.amqp.tmp")
      File.open(tmp_path, "w") do |io|
        io.buffer_size = Config.instance.file_buffer_size
        SchemaVersion.prefix(io, :definition)
        @exchanges.each do |_name, e|
          next if !include_transient && !e.durable
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable, e.auto_delete, e.internal,
            false, AMQP::Table.new(e.arguments))
          io.write_bytes f
        end
        @queues.each do |_name, q|
          next if !include_transient && !q.durable
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
            q.auto_delete, false, AMQP::Table.new(q.arguments))
          io.write_bytes f
        end
        @exchanges.each do |_name, e|
          next if !include_transient && !e.durable
          e.queue_bindings.each do |bt, queues|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            queues.each do |q|
              f = AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, q.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
          e.exchange_bindings.each do |bt, exchanges|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            exchanges.each do |ex|
              f = AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, ex.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
        end
        io.fsync
      end
      File.rename tmp_path, File.join(@data_dir, "definitions.amqp")
    end

    private def save!
      File.open(File.join(@data_dir, "definitions.amqp"), "W") do |f|
        f.seek(0, IO::Seek::End)
        while frame = @save.receive?
          case frame
          when AMQP::Frame::Exchange::Declare
            next unless frame.durable
          when AMQP::Frame::Queue::Declare
            next unless frame.durable
            next if frame.exclusive
          when AMQP::Frame::Exchange::Delete
            next unless @exchanges[frame.exchange_name]?.try(&.durable)
          when AMQP::Frame::Queue::Delete
            next unless @queues[frame.queue_name]?.try { |q| q.durable && !q.exclusive }
          when AMQP::Frame::Queue::Bind, AMQP::Frame::Queue::Unbind
            next unless @exchanges[frame.exchange_name]?.try &.durable
            next unless @queues[frame.queue_name]?.try &.durable
          when AMQP::Frame::Exchange::Bind, AMQP::Frame::Exchange::Unbind
            next unless @exchanges[frame.source]?.try &.durable
            next unless @exchanges[frame.destination]?.try &.durable
          else raise "Cannot apply frame #{frame.class} in vhost #{@name}"
          end
          @log.debug { "Storing definition: #{frame.inspect}" }
          f.write_bytes frame
          f.fsync
        end
      end
      @policies.save!
    rescue ex
      abort "ERROR: Writing definitions. #{ex.inspect}"
    end

    private def load_segments_on_disk
      ids = Array(UInt32).new
      Dir.each_child(@data_dir) do |f|
        if f.starts_with? "msgs."
          ids << f[5, 10].to_u32
        end
      end
      ids.sort!
      was_empty = ids.empty?
      ids << 1_u32 if was_empty
      segments = Hash(UInt32, MFile).new(initial_capacity: Math.pw2ceil(Math.max(ids.size, 16)))
      last_idx = ids.size - 1
      ids.each_with_index do |seg, idx|
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        path = File.join(@data_dir, filename)
        file = if idx == last_idx
                 # expand the last segment
                 MFile.new(path, Config.instance.segment_size)
               else
                 MFile.new(path)
               end
        if was_empty
          SchemaVersion.prefix(file, :message)
        else
          SchemaVersion.verify(file, :message)
        end
        segments[seg] = file
      end
      segments
    end

    private def gc_segments_loop
      # Don't gc all vhosts at the same time
      sleep Random.rand(Config.instance.gc_segments_interval)
      # save some memory if the vhost was created and deleted fast
      return if @closed
      referenced_sps = ReferencedSPs.new(@queues.size)
      loop do
        sleep Config.instance.gc_segments_interval
        next unless @dirty
        break if @closed
        gc_log("collecting sps") do
          collect_sps(referenced_sps)
        end
        gc_log("garbage collecting") do
          gc_segments(referenced_sps)
        end
        gc_log("compact internal queues") do
          @queues.each_value &.compact
        end
        gc_log("GC collect") do
          GC.collect
        end
        @dirty = false
      end
    rescue ex
      @log.fatal("Unhandled exception in #gc_segments_loop, killing process")
      @log.fatal(ex)
      exit 1
    end

    private def gc_log(desc, &blk)
      elapsed = Time.measure do
        mem = Benchmark.memory(&blk)
        @log.info { "GC segments, #{desc} used #{mem.humanize_bytes} memory" }
      end
      return if elapsed.total_milliseconds <= 10
      @log.info { "GC segments, #{desc} took #{elapsed.total_milliseconds} ms" }
    end

    private def collect_sps(referenced_sps) : Nil
      @exchanges.each_value do |ex|
        ex.referenced_sps(referenced_sps)
      end
      @queues.each_value do |q|
        referenced_sps << SPQueue.new(q.unacked)
        referenced_sps << SPQueue.new(q.ready)
      end
    end

    private def gc_segments(referenced_sps) : Nil
      @log.debug { "GC segments" }
      collected = 0_u64

      if referenced_sps.empty?
        @segments.delete_if do |seg, mfile|
          next if mfile == @wfile
          collected += mfile.disk_usage
          @log.info { "Deleting segment #{seg}" }
          @segment_holes.delete(mfile)
          mfile.close(truncate_to_size: false)
          mfile.delete
          true
        end
      end

      file = nil
      prev_sp = SegmentPosition.zero
      referenced_sps.each do |sp|
        next if sp == prev_sp # ignore duplicates

        if prev_sp > sp
          raise ReferencedSPs::NotInOrderError.new(prev_sp, sp)
        end
        # if the last segment was the same as this
        if prev_sp.segment == sp.segment
          # if there's a hole between previous sp and this sp
          # punch a hole
          collected += punch_hole(file.not_nil!, prev_sp.end_position, sp.position)
        else # dealing with a new segment
          # truncate the previous file
          if f = file
            collected += f.truncate(prev_sp.end_position)
          end

          # if a segment is missing between this and previous SP
          # means that a segment is unused, so let's delete it
          ((prev_sp.segment + 1)...sp.segment).each do |seg|
            if mfile = @segments.delete(seg)
              collected += mfile.disk_usage
              @log.info { "Deleting segment #{seg}" }
              @segment_holes.delete(mfile)
              mfile.close(truncate_to_size: false)
              mfile.delete
            end
          end

          file = @segments[sp.segment]

          # punch from start of the segment (but not the version prefix)
          collected += punch_hole(file, sizeof(Int32), sp.position)
        end
        prev_sp = sp
      end

      # truncate the last opened segment to last message
      if file && file != @wfile
        collected += file.truncate(prev_sp.end_position)
      end

      @log.info { "Garbage collected #{collected.humanize_bytes}" } if collected > 0
    end

    # For each file we hold an array of holes where we've already punched
    record Hole, start_pos : UInt32, end_pos : UInt32
    @segment_holes = Hash(MFile, Array(Hole)).new { |h, k| h[k] = Array(Hole).new }

    private def punch_hole(segment, start_pos : Int, end_pos : Int) : Int
      {% unless flag?(:linux) %}
        # only linux supports hole punching (MADV_REMOVE)
        return 0
      {% end %}
      start_pos = start_pos.to_u32
      end_pos = end_pos.to_u32
      hole_size = end_pos - start_pos
      return 0 if hole_size == 0

      holes = @segment_holes[segment]
      if idx = holes.bsearch_index { |hole| hole.start_pos >= start_pos }
        hole = holes[idx]
        hole_start = hole.start_pos
        hole_end = hole.end_pos

        case
        when start_pos == hole_start && hole_end >= end_pos
          # we got the exact same hole or a smaller hole
          return 0
        when start_pos == hole_start && hole_end < end_pos
          # we got a hole that's not as long, then expand the hole
          holes[idx] = Hole.new(start_pos, end_pos)
        when start_pos < hole_start <= end_pos
          # if the next hole is further away, but this hole ends after the next hole starts
          # then expand the hole
          end_pos = Math.max(end_pos, hole_end)
          holes[idx] = Hole.new(start_pos, end_pos)
        when start_pos < end_pos < hole_start
          # this a completely new hole
          holes.insert(idx, Hole.new(start_pos, end_pos))
        else
          raise "Unexpected hole punching case: \
                 start_pos=#{start_pos} end_pos=#{end_pos} \
                 hole_start=#{hole_start} hole_end=#{hole_end}"
        end

        hole_size = end_pos - start_pos
        @log.debug { "Punch hole in #{segment.path}, from #{start_pos}, to #{end_pos}" }
        segment.punch_hole(hole_size, start_pos)
      else
        @log.debug { "Punch hole in #{segment.path}, from #{start_pos}, to #{end_pos}" }
        holes << Hole.new(start_pos, end_pos)
        segment.punch_hole(hole_size, start_pos)
      end
    end

    private def make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        if name.empty?
          DefaultExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        else
          DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        end
      when "fanout"
        FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "x-delayed-message"
        type = arguments.delete("x-delayed-type")
        raise Error::ExchangeTypeError.new("Missing required argument 'x-delayed-type'") unless type
        arguments["x-delayed-message"] = true
        make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      when "x-federation-upstream"
        FederationExchange.new(vhost, name, arguments)
      when "x-consistent-hash"
        ConsistentHashExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      else raise Error::ExchangeTypeError.new("Unknown exchange type #{type}")
      end
    end

    def restore_state
      unless File.exists? File.join(@data_dir, "transient.state")
        @log.warn "No transient.state"
        return
      end
      json = File.open(File.join(@data_dir, "transient.state")) do |f|
        JSON.parse(f)
      end
      state = json.to_h
      connections = state["connections"].as_a
      connections.each do |conn|
        Client.restore_from_json(conn)
        # find connection in json
        tune_ok = ""
        start_ok = ""
        c = Client.new(socket, socket.remote_address, socket.local_address, self, @events, tune_ok, start_ok)
      end
    end

    def save_transient_state : Nil
      File.open(File.join(@data_dir, "transient.state"), "w") do |f|
        JSON.build(f) do |json|
          json.field("connections") do
            json.array do
              @connections.each do |conn|
                conn.save_to_json(json)
              end
            end
          end
        end
      end
    end
  end
end
