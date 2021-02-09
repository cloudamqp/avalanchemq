require "file_utils"
require "./queue"
require "../schema"

module AvalancheMQ
  class DurableQueue < Queue
    @durable = true
    @ack_lock = Mutex.new(:checked)
    @enq_lock = Mutex.new(:checked)
    @ack : MFile

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      if Dir.exists?(@index_dir)
        restore_index
      else
        Dir.mkdir @index_dir
      end
      File.write(File.join(@index_dir, ".queue"), @name)
      @enq = MFile.new(File.join(@index_dir, "enq"), ack_max_file_size)
      SchemaVersion.verify_or_prefix(@enq, :index)
      @enq.seek 0, IO::Seek::End
      @enq.advise(MFile::Advice::DontNeed)
      @ack = MFile.new(File.join(@index_dir, "ack"), ack_max_file_size)
      SchemaVersion.verify_or_prefix(@ack, :index)
      @ack.seek 0, IO::Seek::End
      @ack.advise(MFile::Advice::DontNeed)
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @enq_lock.lock
      @ack_lock.lock
      i = 0
      @enq.close
      @enq = MFile.new(File.join(@index_dir, "enq.tmp"),
                       SP_SIZE * (@ready.size + @unacked.size + 1_000_000))
      SchemaVersion.prefix(@enq, :index)
      @ready.locked_each do |all_ready|
        @unacked.locked_each do |all_unacked|
          next_unacked = all_unacked.next.as?(UnackQueue::Unack).try &.sp
          while sp = all_ready.next.as?(SegmentPosition)
            while next_unacked && next_unacked < sp
              @enq.write_bytes next_unacked
              i += 1
              next_unacked = all_unacked.next.as?(UnackQueue::Unack).try &.sp
            end
            @enq.write_bytes sp
            i += 1
          end
          while next_unacked
            @enq.write_bytes next_unacked
            i += 1
            next_unacked = all_unacked.next.as?(UnackQueue::Unack).try &.sp
          end
        end
      end

      @log.info { "Wrote #{i} SPs to new enq file" }
      @enq.move(File.join(@index_dir, "enq"))
      @enq.advise(MFile::Advice::DontNeed)

      @ack.delete
      @ack.close
      @ack = MFile.new(File.join(@index_dir, "ack"), ack_max_file_size)
      SchemaVersion.prefix(@ack, :index)
      @ack.advise(MFile::Advice::DontNeed)
    ensure
      @ack_lock.unlock
      @enq_lock.unlock
    end

    def close : Bool
      super || return false
      @log.debug { "Closing index files" }
      @enq_lock.synchronize do
        @enq.close
      end
      @ack_lock.synchronize do
        @ack.close
      end
      true
    end

    def delete : Bool
      super || return false
      @log.debug { "Deleting index directory" }
      FileUtils.rm_rf @index_dir
      true
    end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      super || return false
      @enq_lock.synchronize do
        @log.debug { "writing #{sp} to enq" }
        begin
          @enq.write_bytes sp
        rescue IO::EOFError
          @log.debug { "Out of capacity in enq file, resizeing" }
          @enq.resize(@enq.size + ack_max_file_size)
          @enq.write_bytes sp
        end
      end
      true
    end

    protected def delete_message(sp : SegmentPosition, persistent = false) : Nil
      super
      begin
        @ack_lock.synchronize do
          ack_sp = RawSegmentPosition.new(sp)
          @log.debug { "writing #{ack_sp} to ack" }
          @ack.write_bytes ack_sp
        end
      rescue IO::EOFError
        time = Time.measure do
          compact_index!
        end
        @log.info { "Compacting index took #{time.total_milliseconds} ms" }
      end
    end

    def purge
      @log.info "Purging"
      @enq_lock.synchronize do
        @enq.close(truncate_to_size: false)
        @enq = MFile.new(File.join(@index_dir, "enq.tmp"), ack_max_file_size)
        SchemaVersion.prefix(@enq, :index)
        @enq.move(File.join(@index_dir, "enq"))
        @enq.advise(MFile::Advice::DontNeed)
      end
      @ack_lock.synchronize do
        @ack.close(truncate_to_size: false)
        @ack = MFile.new(File.join(@index_dir, "ack.tmp"), ack_max_file_size)
        SchemaVersion.prefix(@ack, :index)
        @ack.move(File.join(@index_dir, "ack"))
        @ack.advise(MFile::Advice::DontNeed)
      end
      super
    end

    def fsync_enq
      return if @closed
      # @log.debug "fsyncing enq"
      @enq.fsync
    end

    def fsync_ack
      return if @closed
      # @log.debug "fsyncing ack"
      @ack.fsync
    end

    SP_SIZE = SegmentPosition::BYTESIZE

    private def restore_index : Nil
      @log.info "Restoring index"
      SchemaVersion.migrate(File.join(@index_dir, "enq"), :index)
      SchemaVersion.migrate(File.join(@index_dir, "ack"), :index)
      File.open(File.join(@index_dir, "enq")) do |enq|
        File.open(File.join(@index_dir, "ack")) do |ack|
          enq.buffer_size = Config.instance.file_buffer_size
          ack.buffer_size = Config.instance.file_buffer_size
          enq.advise(File::Advice::Sequential)
          ack.advise(File::Advice::Sequential)
          SchemaVersion.verify(enq, :index)
          SchemaVersion.verify(ack, :index)

          ack_count = ((ack.size - sizeof(Int32)) // RawSegmentPosition::BYTESIZE).to_u32
          acked = Array(RawSegmentPosition).new(ack_count)
          loop do
            acked << RawSegmentPosition.from_io ack
          rescue IO::EOFError
            break
          end
          acked.sort!
          # to avoid repetetive allocations in Dequeue#increase_capacity
          # we redeclare the ready queue with a larger initial capacity
          enq_count = (enq.size.to_i64 - ack.size - (sizeof(Int32) * 2)) // SP_SIZE
          capacity = Math.max(enq_count, 1024)
          @ready = ready = ReadyQueue.new Math.pw2ceil(capacity)
          loop do
            sp = SegmentPosition.from_io enq
            next if acked.bsearch { |asp| asp >= sp } == sp
            ready << sp
          rescue IO::EOFError
            break
          end
          @log.info { "#{message_count} messages" }
          message_available if message_count > 0
        end
      end
    rescue ex : IO::Error
      @log.error { "Could not restore index: #{ex.inspect}" }
    end

    def enq_file_size
      @enq.size
    end

    def ack_file_size
      @ack.size
    end

    private def ack_max_file_size
      SP_SIZE * Config.instance.queue_max_acks
    end
  end
end
