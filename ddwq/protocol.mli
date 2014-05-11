open Async.Std
open Ddwq
(** The messages sent between the slave nodes and master node *)

(****************************************)
(** { Sending and receiving messages } **)
(****************************************)

(** Send or receive marshaled messages through Reader or Writer *)
module type Marshalable = sig
  (** Message type *)
  type 'b t

  (** [receive] and [send] receive and send messages.  They will raise
      exceptions if the connection is broken or 
      there was some kind of I/O failure (e.g. if the connection was
      unexpectedly terminated). *)
  val receive : Reader.t -> [`Ok of 'b t | `Eof] Deferred.t
  val send    : Writer.t -> 'b t -> unit
end

(***********************)
(** { DDWQ Protocol } **)
(***********************)


module ClientInitResponse : sig
  type 'b t = | InitForWorkTypeFailed of string
              | InitForWorkTypeSucceeded
                
  include Marshalable with type 'b t := 'b t
end

module ClientInitRequest : sig
  type 'b t = InitForWorkType of string
                
  include Marshalable with type 'b t := 'b t
end

module ClientRequest : functor (Work : Ddwq.WorkType)  ->  sig
  type 'b t = DDWQWorkRequest of Work.input

  
  include Marshalable with type 'b t := 'b t
end

module ClientResponse : functor (Work : Ddwq.WorkType)  -> sig
  type 'b t = DDWQWorkResult of Work.output

  
  include Marshalable with type 'b t := 'b t
end

module ChainComm_ReplicaNodeRequest : sig 
  type 'b t_table = (int, 'b) Hashtbl.t
  type 'b t =  | GetReadyToSync
            | SyncDone
            | UpdateYourHistory of (int * ('b t_table)) (*Seq num , History for seq num*)
            | TakeThisUpdate of (int * 'b)
            | TakeThisACK of (int)


  include Marshalable with type 'b t := 'b t
end

module ChainComm_ReplicaNodeResponse : sig
  type 'b t = DoSyncForState of ((*Last acked seq num/current data state*)int *(*last sent seq number to T+*)int)

  include Marshalable with type 'b t := 'b t
end

module MasterMonitorComm : sig
  type 'b t =  |ImAlive 
            | YouAreNewHead | YouAreNewTail 
            | YouHaveNewPrevNode of ((string * int) * int) | YouHaveNewNextNode of (string * int)
            | OnSeqNumber of int
            | PrepareNewTail of (string * int)

  include Marshalable with type 'b t := 'b t
end

module MasterServiceAck : sig
  type 'b t = FirstChainMemberAck | NewTailAck

  include Marshalable with type 'b t := 'b t
end

module MasterServiceRequest : sig
  type 'b t = InitRequest of (string * int)

  include Marshalable with type 'b t := 'b t
end

module MasterServiceResponse : sig
  type 'b t =  |FirstChainMember | NewTail 
            | InitDone | InitFailed

  include Marshalable with type 'b t := 'b t
end


(** Messages from the master to the slave *)
module SlaveRequest (Work : Ddwq.WorkType) : sig
  type 'b t = DoWorkRequest of Work.input
      (** Execute the  *)

  (** You can send and receive [WorkerRequest(J).t]s by calling
      [WorkerRequest(J).send] and [receive] respectively.  These functions are
      inherited from {!Marshalable}: *)
  include Marshalable with type 'b t := 'b t
end

(** Messages from the worker to the controller *)
module SlaveResponse (Work : Ddwq.WorkType) : sig
  type 'b t =
    | DoWorkFailed of string
      (** Execution threw the given exception with stacktrace *)

    | DoWorkResult of Work.output
      (** Execution was successful and yeildied an output *)

  (** You can send and receive [WorkerRequest(J).t]s by calling
      [WorkerRequest(J).send] and [receive] respectively.  These functions are
      inherited from {!Marshalable}: *)
  include Marshalable with type 'b t := 'b t
end

