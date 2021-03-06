require "./libc"

# No PR yet
class File
  def punch_hole(size, offset = 0, keep_size = true)
    {% if flag?(:linux) %}
      flags = LibC::FALLOC_FL_PUNCH_HOLE
      flags |= LibC::FALLOC_FL_KEEP_SIZE if keep_size
      if LibC.fallocate(fd, flags, offset, size) != 0
        raise File::Error.from_errno("fallocate", file: @path)
      end
    {% end %}
  end

  def allocate(size, offset = 0, keep_size = false)
    {% if flag?(:linux) %}
      flags = case
              when keep_size then LibC::FALLOC_FL_KEEP_SIZE
              end
      if LibC.fallocate(fd, flags, offset, size) != 0
        raise File::Error.from_errno("fallocate", file: @path)
      end
    {% end %}
  end

  def advise(advice : Advice)
    {% if flag?(:linux) %}
      if LibC.posix_fadvise(fd, 0, 0, advice) != 0
        raise File::Error.from_errno("fadvise", file: @path)
      end
    {% end %}
  end

  enum Advice
    Normal
    Random
    Sequential
    WillNeed
    DontNeed
    NoReuse
  end
end

class IO::FileDescriptor
  def write_at(buffer, offset) : Int64
    bytes_written = LibC.pwrite(fd, buffer, buffer.size, offset)

    if bytes_written == -1
      raise IO::Error.from_errno "Error writing file"
    end

    bytes_written.to_i64
  end

  # In-kernel copy between two file descriptors
  # using the copy_file_range syscall
  def copy_range_from(src : self, length : Int) : Int64
    {% if LibC.has_method?(:copy_file_range) %}
      length = length.to_i64
      remaining = length
      flush
      src.seek(0, IO::Seek::Current) unless src.@in_buffer_rem.empty?
      while remaining > 0
        len = LibC.copy_file_range(src.fd, nil, fd, nil, remaining, 0)
        if len == -1
          raise IO::Error.from_errno "copy_file_range"
        end
        break if len.zero?
        remaining -= len
      end
      length - remaining
    {% else %}
      IO.copy(src, self, length)
    {% end %}
  end
end
