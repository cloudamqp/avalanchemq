module AvalancheMQ
  abstract struct BaseSegmentPosition
    include Comparable(self)
    getter segment : UInt32
    getter position : UInt32

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32)
    end

    def initialize(sp : BaseSegmentPosition)
      @segment = sp.segment
      @position = sp.position
    end

    def <=>(other : BaseSegmentPosition)
      r = segment <=> other.segment
      return r unless r.zero?
      position <=> other.position
    end

    def to_s(io : IO)
      io << @segment.to_s.rjust(10, '0')
      io << @position.to_s.rjust(10, '0')
    end

    def to_i64
      ((segment.to_i64 << 32) | position).to_i64
    end

    def self.zero
      self.new(0_u32, 0_u32)
    end

    def self.from_i64(i : Int64)
      seg = i.bits(32..)
      pos = i.bits(0..31)
      self.new(seg.to_u32, pos.to_u32)
    end

    def self.parse(s)
      raise ArgumentError.new("A SegmentPosition string has to be 20 chars long") if s.bytesize != 20
      seg = s[0, 10].to_u32
      pos = s[10, 10].to_u32
      self.new seg, pos
    end

    def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
      raise "Method not implemented"
    end

    def self.make
      raise "Method not implemented"
    end

    abstract def to_io(io : IO, format)
  end

  struct RawSegmentPosition < BaseSegmentPosition
    include Comparable(self)
    BYTESIZE = 8

    def to_io(io : IO, format)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      io.write(slice)
    end

    def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
      seg = UInt32.from_io(io, format)
      pos = UInt32.from_io(io, format)
      self.new(seg, pos)
    end
  end

  struct SegmentPosition < BaseSegmentPosition
    include Comparable(self)
    @[Flags]
    enum SPFlags : UInt8
      HasDLX
    end

    getter bytesize : UInt32
    getter priority : UInt8
    getter flags : SPFlags
    getter expiration_ts : Int64
    BYTESIZE = 22

    def initialize(@segment : UInt32, @position : UInt32, @bytesize = 0_u32, @expiration_ts = 0_i64, @priority = 0_u8, @flags = SPFlags.new(0_u8))
      super(@segment, @position)
    end

    def end_position
      @position + @bytesize
    end

    def to_io(io : IO, format)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      format.encode(@bytesize, slice[8, 4])
      format.encode(@expiration_ts, slice[12, 8])
      slice[20] = @priority
      slice[21] = @flags.value
      io.write(slice)
    end

    def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
      seg = UInt32.from_io(io, format)
      pos = UInt32.from_io(io, format)
      bytesize = UInt32.from_io(io, format)
      ts = Int64.from_io(io, format)
      priority = io.read_byte || raise IO::EOFError.new
      flags_value = io.read_byte || raise IO::EOFError.new
      flags = SPFlags.from_value flags_value
      self.new(seg, pos, bytesize, ts, priority, flags)
    end

    def self.make(segment, position, msg)
        expires_at =
          if delay = msg.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
            msg.timestamp + delay.to_i64
          elsif exp_ms = msg.properties.expiration.try(&.to_i64?)
            msg.timestamp + exp_ms
          else
            0_i64
          end
        priority = msg.properties.priority || 0_u8
        flags =
          if msg.properties.headers.try(&.has_key?("x-dead-letter-exchange"))
            SPFlags::HasDLX
          else
            SPFlags.new(0u8)
          end
        self.new(segment, position, msg.bytesize.to_u32, expires_at, priority, flags)
    end
  end
end
