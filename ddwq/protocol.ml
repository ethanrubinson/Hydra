open Async.Std

(************************************)
(** { Marshaling and unmarshaling } *)
(************************************)

module type Marshalable = sig
  type 'b t
  val receive : Reader.t -> 'b t Reader.Read_result.t Deferred.t
  val send    : Writer.t -> 'b t -> unit
end

(** Utility class for implementing the Marshalable interface *)
module Marshaller = struct
  let receive r = Reader.read_marshal r
  let send w v  = Writer.write_marshal w [] v
end

(*************************)
(** { Protocol messages} *)
(*************************)

module ChainComm_ReplicaNodeRequest = struct
  type 'b t_table = (int, 'b) Hashtbl.t
  type 'b t =  | GetReadyToSync
            | SyncDone
            | UpdateYourHistory of (int * ('b t_table)) (*Seq num , History for seq num*)
            | TakeThisUpdate of (int * 'b)
            | TakeThisACK of (int)


  include Marshaller
end

module ChainComm_ReplicaNodeResponse = struct
  type 'b t = DoSyncForState of ((*Last acked seq num/current data state*)int *(*last sent seq number to T+*)int)

  include Marshaller
end

module MasterMonitorComm = struct
  type 'b t =  | ImAlive 
            | YouAreNewHead | YouAreNewTail 
            | YouHaveNewPrevNode of ((string * int) * int) | YouHaveNewNextNode of (string * int)
            | OnSeqNumber of int
            | PrepareNewTail of (string * int)

  include Marshaller
end

module MasterServiceAck = struct
  type 'b t = FirstChainMemberAck | NewTailAck

  include Marshaller
end

module MasterServiceRequest = struct
  type 'b t = InitRequest of (string * int)  (*hostname * listening port *)

  include Marshaller
end

module MasterServiceResponse = struct
  type 'b t = FirstChainMember | NewTail | InitDone | InitFailed

  include Marshaller
end


module SlaveRequest (Work : Ddwq.WorkType) = struct
  type 'b t = DoWorkRequest of Work.input

  include Marshaller
end

module SlaveResponse (Work : Ddwq.WorkType) = struct
  type 'b t =
    | DoWorkFailed of string
    | DoWorkResult of Work.output

  include Marshaller
end

