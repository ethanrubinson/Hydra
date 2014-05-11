open Async.Std 

module Make : functor (Work : Ddwq.WorkType) -> sig
  val run : Work.input -> Ddwq.net_data Deferred.t
end
