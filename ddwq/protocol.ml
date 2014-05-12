open Async.Std
open Ddwq
(************************************)
(** { Marshaling and unmarshaling } *)
(************************************)

module type Marshalable = sig
  type t
  val receive : Reader.t -> t Reader.Read_result.t Deferred.t
  val send    : Writer.t -> t -> unit
end

(** Utility class for implementing the Marshalable interface *)
module Marshaller = struct
  let receive r = Reader.read_marshal r
  let send w v  = Writer.write_marshal w [] v
end

(*************************)
(** { Protocol messages} *)
(*************************)

module ClientInitResponse  = struct
  type t = | InitForWorkTypeFailed of string
              | InitForWorkTypeSucceeded

  include Marshaller
end

module ClientInitRequest  = struct
  type user_id = string
  type work_id = int
  type t = InitForWorkType of (user_id * work_id * string)

  include Marshaller
end

module ClientRequest = functor (Work : Ddwq.WorkType)  -> struct
  type t = DDWQWorkRequest of Work.input

  include Marshaller
end

module ClientResponse = functor (Work : Ddwq.WorkType) -> struct
  type t = DDWQWorkResult of Work.output option

  include Marshaller
end

module ChainComm_ReplicaNodeRequest = struct
  type  t_table = (int, Ddwq.net_data) Hashtbl.t
  type t =  | GetReadyToSync
            | SyncDone
            | UpdateYourHistory of (int *  t_table) (*Seq num , History for seq num*)
            | TakeThisUpdate of (int * Ddwq.net_data)  
            | TakeThisACK of (int)


  include Marshaller
end

module ChainComm_ReplicaNodeResponse = struct
  type  t = DoSyncForState of ((*Last acked seq num/current data state*)int *(*last sent seq number to T+*)int)

  include Marshaller
end

module MasterMonitorComm = struct
  type t =  | ImAlive 
            | YouAreNewHead | YouAreNewTail 
            | YouHaveNewPrevNode of ((string * int) * int) | YouHaveNewNextNode of (string * int)
            | OnSeqNumber of int
            | PrepareNewTail of (string * int)

  include Marshaller
end

module MasterServiceAck = struct
  type t = FirstChainMemberAck | NewTailAck

  include Marshaller
end

module MasterServiceRequest = struct
  type t = InitRequest of (string * int)  (*hostname * listening port *)

  include Marshaller
end

module MasterServiceResponse = struct
  type t = FirstChainMember | NewTail | InitDone | InitFailed

  include Marshaller
end


module SlaveRequest (Work : Ddwq.WorkType) = struct
  type t = DoWorkRequest of Work.input

  include Marshaller
end

module SlaveResponse (Work : Ddwq.WorkType) = struct
  type t =
    | DoWorkFailed of string
    | DoWorkResult of Work.output

  include Marshaller
end

