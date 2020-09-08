module AvalancheMQ
  class ReferenceCounter(T)
    def initialize
      @counter = Hash(T, UInt32).new { 0_u32 }
    end

    def inc(v : T) : UInt32
      @counter[v] += 1
    end

    def dec(v : T) : UInt32
      @counter[v] -= 1
    end

    def each
      @counter.each do |k, v|
        yield k, v
      end
    end

    def each
      @counter.each
    end

    # Deletes all zero referenced keys
    def gc!
      @counter.delete_if { |_, v| v.zero? }
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end

    def clear
      @counter.clear
    end
  end

  # A reference counter which performs an action
  # when the counter goes down to zero again
  class ZeroReferenceCounter(T)
    def initialize(&blk : T -> Nil)
      @on_zero = blk
      @counter = Hash(T, UInt32).new
      @lock = Mutex.new(:unchecked)
    end

    def []=(k : T, v : Int)
      @lock.synchronize do
        @counter[k] = v.to_u32
      end
    end

    def inc(k : T) : UInt32
      @lock.synchronize do
        v = @counter.fetch(k, 0_u32)
        @counter[k] = v + 1
      end
    end

    def dec(k : T) : UInt32
      @lock.synchronize do
        if v = @counter.fetch(k, nil)
          cnt = @counter[k] = v - 1
          if cnt.zero?
            @counter.delete k
            @on_zero.call k
          end
          cnt
        else
          raise KeyError.new("Missing key #{k}")
        end
      end
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end
  end

  # A reference counter which performs an action
  # when the counter goes down to zero again
  class SafeReferenceCounter(T)
    def initialize
      @counter = Deque(T).new(131_072)
      @lock = Mutex.new(:unchecked)
    end

    def []=(k : T, v : Int)
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x > k }
          v.times { |i| @counter.insert(idx + i, k) }
        else
          v.times { @counter.push(k) }
        end
      end
    end

    def inc(k : T) : Nil
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x > k }
          @counter.insert(idx, k)
        else
          @counter.push(k)
        end
      end
    end

    def dec(k : T) : Nil
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x >= k }
          @counter.delete_at idx
          # if no more occurences
          unless @counter[idx]? == k
            @zero_refs.push k
          end
        else
          raise KeyError.new("Missing key #{k}")
        end
      end
    end

    @zero_refs = Array(T).new(131_072)

    # Yield and delete all zero referenced keys
    def empty_zero_referenced!
      @lock.synchronize do
        @zero_refs.each do |sp|
          yield sp
        end
        @counter = Deque(T).new(@counter.size) { |i| @counter[i] }
        @zero_refs = Array(T).new(131_072)
      end
    end

    def referenced_segments(set)
      @lock.synchronize do
        prev = nil
        @counter.each do |sp|
          if sp.segment != prev
            set << sp.segment
            prev = sp.segment
          end
        end
      end
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end
  end
end
