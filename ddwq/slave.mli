(** The interface to the worker server. *)

open Async.Std

module Make (Work : Ddwq.WorkType) : sig
  (** Handle the requests for a single connection.  The Reader and Writer should
      be used to send and receive messages of type WorkerResponse(Job) and
      WorkerRequest(Job). *)
  val run : Reader.t -> Writer.t -> unit Deferred.t
end

(** Start the worker server.  *)
val init : int -> unit Deferred.t

