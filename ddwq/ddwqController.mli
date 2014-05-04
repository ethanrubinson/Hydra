open Async.Std 

module Make : functor (Work : Ddwq.WorkType) -> sig
  val run : unit -> unit Deferred.t
end

(** set up the map reduce controller to connect the the provided worker adresses *)
val init : (string * int) list -> unit