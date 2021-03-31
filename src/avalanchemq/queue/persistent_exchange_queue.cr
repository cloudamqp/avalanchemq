require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true

    def initialize(vhost : VHost, name : String, args, ready)
      args["x-overflow"] = "drop-head"
      super(vhost, name, false, false, args, ready)
    end

    def head(count : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if count < 0
        count = q_size + count
        return if count < 0
        @ready.each(0, count, &blk)
      else
        @ready.each(0, count, &blk)
      end
    end

    def tail(count : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if count < 0
        @ready.each(count.abs, q_size, &blk)
      else
        @ready.each(Math.max(0, q_size - count), q_size, &blk)
      end
    end

    def from(offset : Int64, &blk : SegmentPosition -> Nil)
      return @ready.each(&blk) if offset == 0
      start_sp = SegmentPosition.from_i64(offset)
      i = @ready.bsearch_index { |sp| sp >= start_sp } || 0
      @ready.each(i, @ready.size, &blk)
    end
  end
end
