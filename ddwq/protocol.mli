open Async.Std

(** The messages sent between the slave nodes and master node *)

(****************************************)
(** { Sending and receiving messages } **)
(****************************************)

(** Send or receive marshaled messages through Reader or Writer *)
module type Marshalable = sig
  (** Message type *)
  type t

  (** [receive] and [send] receive and send messages.  They will raise
      exceptions if the connection is broken or 
      there was some kind of I/O failure (e.g. if the connection was
      unexpectedly terminated). *)
  val receive : Reader.t -> [`Ok of t | `Eof]  Deferred.t
  val send    : Writer.t -> t -> unit
end

(***********************)
(** { DDWQ Protocol } **)
(***********************)

module MasterHeartbeat: sig
  type t = ImAlive

  include Marshalable with type t := t
end

module MasterServiceAck : sig
  type t = FirstChainMemberAck | NewTailAck

  include Marshalable with type t := t
end

module MasterServiceRequest : sig
  type t = InitRequest of (string * int)

  include Marshalable with type t := t
end

module MasterServiceResponse : sig
  type t = FirstChainMember | NewTail | PrepareNewTail of (string * int) | InitDone | InitFailed

  include Marshalable with type t := t
end


(** Messages from the master to the slave *)
module SlaveRequest (Work : Ddwq.WorkType) : sig
  type t = DoWorkRequest of Work.input
      (** Execute the  *)

  (** You can send and receive [WorkerRequest(J).t]s by calling
      [WorkerRequest(J).send] and [receive] respectively.  These functions are
      inherited from {!Marshalable}: *)
  include Marshalable with type t := t
end

(** Messages from the worker to the controller *)
module SlaveResponse (Work : Ddwq.WorkType) : sig
  type t =
    | DoWorkFailed of string
      (** Execution threw the given exception with stacktrace *)

    | DoWorkResult of Work.output
      (** Execution was successful and yeildied an output *)

  (** You can send and receive [WorkerRequest(J).t]s by calling
      [WorkerRequest(J).send] and [receive] respectively.  These functions are
      inherited from {!Marshalable}: *)
  include Marshalable with type t := t
end

