open Async.Std
open Ddwq
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
  val receive : Reader.t -> [`Ok of t | `Eof] Deferred.t
  val send    : Writer.t -> t -> unit
end

(***********************)
(** { DDWQ Protocol } **)
(***********************)

module ClientToMasterRequest : sig
  type t =  HeadAndTailRequest
                
  include Marshalable with type t := t
end

module ClientToMasterResponse : sig
  type t = | HeadAndTailResponse of ((string * int) * (string * int)) option (*ip * port req -> ip * port resp*)
                
  include Marshalable with type t := t
end


module ClientInitResponse : sig
  type t = | InitForWorkTypeFailed of string
              | InitForWorkTypeSucceeded
                
  include Marshalable with type t := t
end

module ClientInitRequest : sig
  type user_id = string
  type work_id = int
  type t = InitForWorkType of  (user_id * work_id * string)
                
  include Marshalable with type t := t
end

module ClientRequest : functor (Work : Ddwq.WorkType)  ->  sig
  type t = DDWQWorkRequest of Work.input

  
  include Marshalable with type t := t
end

module ClientResponse : functor (Work : Ddwq.WorkType)  -> sig
  type t = DDWQWorkResult of Work.output option

  
  include Marshalable with type t := t
end

module ChainComm_ReplicaNodeRequest : sig 
  type t_table = (int, net_data) Hashtbl.t
  type t =  | GetReadyToSync
            | SyncDone
            | UpdateYourHistory of (int *  t_table) (*Seq num , History for seq num*)
            | TakeThisUpdate of (int * net_data)
            | TakeThisACK of (int)


  include Marshalable with type t := t
end

module ChainComm_ReplicaNodeResponse : sig
  type t = DoSyncForState of ((*Last acked seq num/current data state*)int *(*last sent seq number to T+*)int)

  include Marshalable with type t :=  t
end

module MasterMonitorComm : sig
  type t =  |ImAlive 
            | YouAreNewHead | YouAreNewTail 
            | YouHaveNewPrevNode of ((string * int) * int) | YouHaveNewNextNode of (string * int)
            | OnSeqNumber of int
            | PrepareNewTail of (string * int)

  include Marshalable with type t := t
end

module MasterServiceAck : sig
  type t = FirstChainMemberAck | NewTailAck

  include Marshalable with type t := t
end

module MasterServiceRequest : sig
  type t = InitRequest of (string * int * (int * int))

  include Marshalable with type t := t
end

module MasterServiceResponse : sig
  type t =  |FirstChainMember | NewTail 
            | InitDone | InitFailed

  include Marshalable with type t := t
end


(** Messages from the master to the slave *)
module SlaveRequest (Work : Ddwq.WorkType) : sig
  type t = DoWorkRequest of Work.input
      (** Execute the  *)

  (** You can send and receive [WorkerRequest(J).t]s by calling
      [WorkerRequest(J).send] and [receive] respectively.  These functions are
      inherited from {!Marshalable}: *)
  include Marshalable with type  t :=  t
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

