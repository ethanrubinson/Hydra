open Async.Std 

module Make : functor (Work : Ddwq.WorkType) -> sig
  val run : unit -> Ddwq.net_data Deferred.t
end
