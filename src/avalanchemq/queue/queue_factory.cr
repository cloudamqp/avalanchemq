require "./queue"
require "./priority_queue"
require "./durable_queue"

module AvalancheMQ
  class QueueFactory
    @@locks = Hash(String, Mutex).new

    def self.make(vhost : VHost, frame : AMQP::Frame)
      if prio_queue? frame
        make(vhost, frame, Queue::PriorityReadyQueue.new)
      else
        make(vhost, frame, Queue::ReadyQueue.new)
      end
    end

    def self.make(vhost : VHost, frame : AMQP::Frame, rq : Queue::ReadyQueue)
      if frame.durable
        if prio_queue? frame
          DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h, rq)
        else
          DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h, rq)
        end
      else
        if prio_queue? frame
          PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h, rq)
        else
          Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h, rq)
        end
      end
    end

    # When reading up durable queues we want to reuse the buffer to keep it faster
    def self.make_buffered(vhost : VHost, frame : AMQP::Frame, ack_b : Array(SegmentPosition), ready_q : Queue::ReadyQueue)
      index_dir = File.join(vhost.data_dir, Digest::SHA1.hexdigest frame.queue_name)
      rq = if Dir.exists?(index_dir)
             lock = @@locks[vhost.name] = @@locks[vhost.name]? || Mutex.new
             lock.synchronize do
               vhost.log.info "Restoring index queue=#{frame.queue_name}"
               ack_b.clear
               ready_q.clear
               restore_index(index_dir, ack_b, ready_q)
               vhost.log.info "Restored #{ready_q.size} messages queue=#{frame.queue_name}"
               ready_q.dup
             end
           else
             Queue::ReadyQueue.new
           end
      make(vhost, frame, rq)
    end

    def self.make_persistent_queue(vhost : VHost, name : String, args : Hash(String, AMQP::Field))
      index_dir = File.join(vhost.data_dir, Digest::SHA1.hexdigest name)
      rq = if Dir.exists?(index_dir)
             restore_index(index_dir, Array(SegmentPosition).new, Queue::ReadyQueue.new)
           else
             Queue::ReadyQueue.new
           end
      PersistentExchangeQueue.new(vhost, name, args, rq)
    end

    def self.make_delayed_exchange_queue(vhost, name, arguments)
      index_dir = File.join(vhost.data_dir, Digest::SHA1.hexdigest name)

      rq_buf = Queue::ExpirationReadyQueue.new

      rq = if Dir.exists?(index_dir)
             restore_index(index_dir, Array(SegmentPosition).new, rq_buf)
           else
             rq_buf
           end
      DurableDelayedExchangeQueue.new(vhost, name, false, false, arguments, rq)
    end

    private def self.prio_queue?(frame)
      if value = frame.arguments["x-max-priority"]?
        p_value = value.as?(Int) || raise Error::PreconditionFailed.new("x-max-priority must be an int")
        unless p_value >= 0 && p_value <= 255
          raise Error::PreconditionFailed.new("x-max-priority must be between 0 and 255")
        end
        true
      end
    end

    private def self.restore_index(dir, acked, ready)
      SchemaVersion.migrate(File.join(dir, "enq"), :index)
      SchemaVersion.migrate(File.join(dir, "ack"), :index)
      File.open(File.join(dir, "enq")) do |enq|
        File.open(File.join(dir, "ack")) do |ack|
          enq.buffer_size = Config.instance.file_buffer_size
          ack.buffer_size = Config.instance.file_buffer_size
          enq.advise(File::Advice::Sequential)
          ack.advise(File::Advice::Sequential)
          SchemaVersion.verify(enq, :index)
          SchemaVersion.verify(ack, :index)

          loop do
            sp = SegmentPosition.from_io ack
            if sp.zero?
              File.open(ack.path, "W") do |f|
                f.truncate(ack.pos - SegmentPosition::BYTESIZE)
              end
              break
            end
            acked << sp
          rescue IO::EOFError
            break
          end
          acked.try &.sort!

          loop do
            sp = SegmentPosition.from_io enq
            if sp.zero?
              File.open(enq.path, "W") do |f|
                f.truncate(enq.pos - SegmentPosition::BYTESIZE)
              end
              break
            end
            next if acked.try { |a| a.bsearch { |asp| asp >= sp } == sp }
            ready << sp
          rescue IO::EOFError
            break
          end
          ready
        end
      end
    end
  end
end
