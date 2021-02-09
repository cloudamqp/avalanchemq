module AvalancheMQ
  class OutdatedSchemaVersion < Exception
    getter version : Int32

    def initialize(@version)
      super "Outdated schema version #{@version}"
    end
  end

  class UnsupportedSchemaVersion < Exception
    getter version : Int32

    def initialize(@version, type)
      super "cannot migrate #{type} file from version #{@version}"
    end
  end

  class SchemaVersion
    VERSIONS = {
      definition: 1,
      message: 1,
      index: 3,
    }

    def self.verify(file, type) : Int32
      version = file.read_bytes Int32
      if version != VERSIONS[type]
        raise OutdatedSchemaVersion.new version
      end
      version
    end

    def self.prefix(file, type) : Int32
      version = VERSIONS[type]
      file.write_bytes version
      file.flush
      version
    end

    def self.verify_or_prefix(file, type)
      verify(file, type)
    rescue IO::EOFError
      prefix(file, type)
    end

    def self.migrate(path : String, type)
      File.open(path) do |file|
        begin
          self.verify(file, type)
        rescue ex : OutdatedSchemaVersion
          self.migrate(file, type, ex.version)
        end
      end
    end

    def self.migrate(file, type, current_version)
      case type
      when :index
        case current_version
        when 1
          MigrateIndexV1toV2.run(file)
          return
        when 2
          MigrateAckIndexV2toV3.run(file)
          return
        when 3
          return
        end
      end
      raise UnsupportedSchemaVersion.new current_version, type
    end

    class MigrateIndexV1toV2
      def self.run(file)
        data_dir = File.join(File.dirname(file.path), "..") # data dir is one level up from index files
        File.open("#{file.path}.tmp", "w") do |f|
          SchemaVersion.prefix(f, :index)
          prev_segment = 0u32
          seg = nil
          loop do
            sp_v1 =
              begin
                SegmentPositionV1.from_io file
              rescue IO::EOFError
                break
              end
            if prev_segment != sp_v1.segment
              seg.try &.close
              seg = open_segment data_dir, sp_v1.segment
            end
            if segment = seg
              segment.pos = sp_v1.position
              begin
                msg = MessageMetadata.from_io seg.not_nil!
                sp = SegmentPosition.make(sp_v1.segment, sp_v1.position, msg)
                f.write_bytes sp
              rescue IO::EOFError
                # if the message has been truncated by GC already
                next
              end
            else
              # if the file has been deleted by GC already
              next
            end
          end
          seg.try &.close
          File.rename f.path, file.path
          f.fsync
        end
      end

      private def self.open_segment(data_dir, seg)
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        file = File.new(File.join(data_dir, filename))
        file.buffer_size = Config.instance.file_buffer_size
        file
      rescue File::NotFoundError
        nil
      end

      struct SegmentPositionV1
        getter segment : UInt32
        getter position : UInt32

        def initialize(@segment : UInt32, @position : UInt32)
        end

        def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
          seg = UInt32.from_io(io, format)
          pos = UInt32.from_io(io, format)
          # SegmentPosition at schema version 1 included also bytesize, expiration_ts and priority
          # skipping them as we don't need them
          io.skip(sizeof(UInt32) + sizeof(Int64) + sizeof(UInt8))
          self.new(seg, pos)
        end
      end
    end

    class MigrateAckIndexV2toV3
      def self.run(file)
        return noop_migration(file, :index) if File.basename(file.path) == "enq" #NOOP for enq file
        data_dir = File.join(File.dirname(file.path), "..") # data dir is one level up from index files
        File.open("#{file.path}.tmp", "w") do |f|
          SchemaVersion.prefix(f, :index)
          prev_segment = 0u32
          seg = nil
          loop do
            sp_v2 =
              begin
                SegmentPosition.from_io file
              rescue IO::EOFError
                break
              end
            if prev_segment != sp_v2.segment
              seg.try &.close
              seg = open_segment data_dir, sp_v2.segment
            end
            if segment = seg
              segment.pos = sp_v2.position
              begin
                sp = RawSegmentPosition.new(sp_v2.segment, sp_v2.position)
                f.write_bytes sp
              rescue IO::EOFError
                # if the message has been truncated by GC already
                next
              end
            else
              # if the file has been deleted by GC already
              next
            end
          end
          seg.try &.close
          File.rename f.path, file.path
          f.fsync
        end
      end

      private def self.noop_migration(file, type)
        File.open("#{file.path}.tmp", "w") do |f|
          f.write_bytes VERSIONS[type]
          File.rename f.path, file.path
          f.fsync
        end
      end

      private def self.open_segment(data_dir, seg)
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        file = File.new(File.join(data_dir, filename))
        file.buffer_size = Config.instance.file_buffer_size
        file
      rescue File::NotFoundError
        nil
      end
    end
  end
end
