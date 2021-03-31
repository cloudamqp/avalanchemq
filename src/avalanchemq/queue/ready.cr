require "../segment_position"

module AvalancheMQ
  class Queue
    # ReadyQueue is a sorted Deque of SegmentPositions
    class ReadyQueue
      @lock = Mutex.new(:reentrant)
      @inital_capacity : Int32

      def initialize(inital_capacity = 1024)
        @inital_capacity = inital_capacity.to_i32
        @ready = Deque(SegmentPosition).new(@inital_capacity)
      end

      def initialize(@ready : Deque(SegmentPosition))
        @inital_capacity = @ready.capacity
      end

      def includes?(sp)
        @ready.includes?(sp)
      end

      def shift
        @lock.synchronize do
          @ready.shift
        end
      end

      def shift?
        @lock.synchronize do
          @ready.shift?
        end
      end

      # Shift until block breaks or it returns false
      # If broken with false yield, return the message to the queue
      def shift(&blk : SegmentPosition -> Bool)
        @lock.synchronize do
          while sp = @ready.shift?
            ok = yield sp
            unless ok
              @ready.unshift sp
              break
            end
          end
        end
      end

      # Yields an iterator over all SPs, the deque is locked
      # while it's being read from
      def with_all(&blk : Iterator(SegmentPosition) -> Nil)
        @lock.synchronize do
          yield @ready.each
        end
      end

      # Iterate over all SPs in the deque, locking while reading
      def each(&blk)
        @lock.synchronize do
          @ready.each { |sp| yield sp }
        end
      end

      def each(start : Int, count : Int, &blk)
        @lock.synchronize do
          @ready.each(start: start, count: count) { |sp| yield sp }
        end
      end

      def locked_each(&blk)
        @lock.synchronize do
          yield @ready.each
        end
      end

      def bsearch_index(&blk)
        @lock.synchronize do
          @ready.bsearch_index { |sp, i| yield sp, i }
        end
      end

      # insert a SP, keeps the deque sorted
      # returns SPs in the deque after the operation
      def insert(sp : SegmentPosition)
        @lock.synchronize do
          if i = @ready.bsearch_index { |rsp| rsp > sp }
            @ready.insert(i, sp)
          else
            @ready.push(sp)
          end
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          sps.reverse_each do |sp|
            if i = @ready.bsearch_index { |rsp| rsp > sp }
              @ready.insert(i, sp)
            else
              @ready.push(sp)
            end
          end
          @ready.size
        end
      end

      # Deletes a SP somewhere in the deque
      # returns true/false whether found
      def delete(sp) : Bool
        return false if @ready.empty?
        @lock.synchronize do
          if @ready.first == sp
            @ready.shift
            return true
          else
            if idx = @ready.bsearch_index { |rsp| rsp >= sp }
              if @ready[idx] == sp
                @ready.delete_at(idx)
                return true
              end
            end
          end
        end
        false
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @ready.size > size
            sp = @ready.shift? || break
            yield sp
          end
        end
      end

      def limit_byte_size(bytesize, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @ready.sum(&.bytesize) > bytesize
            sp = @ready.shift? || break
            yield sp
          end
        end
      end

      # Pushes a SP to the end of the deque
      # Returns number of SPs in the deque
      def push(sp : SegmentPosition) : Int32
        @lock.synchronize do
          @ready.push(sp)
          @ready.size
        end
      end

      # alias for `push`
      def <<(sp)
        push(sp)
      end

      def first?
        @ready[0]?
      end

      def [](idx)
        @ready[idx]
      end

      def []?(idx)
        @ready[idx]?
      end

      def empty?
        @ready.empty?
      end

      # yields all messages, then clears it
      # returns number of messages in the queue before purge
      def purge
        @lock.synchronize do
          count = @ready.size
          if @ready.capacity == @inital_capacity
            @ready.clear
          else
            @ready = Deque(SegmentPosition).new(@inital_capacity)
            GC.collect
          end
          count
        end
      end

      def clear
        @ready.clear
      end

      def size
        @ready.size
      end

      def capacity
        @ready.capacity
      end

      def sum(&blk : SegmentPosition -> _) : UInt64
        @ready.sum(0_u64, &blk)
      end

      def compact
        @lock.synchronize do
          @ready = Deque(SegmentPosition).new(@ready.size) { |i| @ready[i] }
        end
      end

      def lock
        @lock.lock
      end

      def unlock
        @lock.unlock
      end

      def to_a
        @ready.to_a
      end

      def dup
        self.class.new(@ready.dup)
      end
    end

    abstract class SortedReadyQueue < ReadyQueue
      def push(sp : SegmentPosition) : Int32
        insert(sp)
      end

      def insert(sp : SegmentPosition)
        @lock.synchronize do
          insert_sorted(sp)
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          sps.each do |sp|
            insert_sorted(sp)
          end
          @ready.size
        end
      end
    end

    class ExpirationReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        idx = @ready.bsearch_index do |rsp|
          rsp.expiration_ts > sp.expiration_ts
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end

    class PriorityReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        idx = @ready.bsearch_index do |rsp|
          rsp.priority < sp.priority
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end
  end
end
