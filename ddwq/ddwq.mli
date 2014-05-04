(** The core Ddwq types --- Work, App, and Controller *)

open Async.Std

type id = string

(******************************************************************************)
(** {2 Job interface}                                                         *)
(******************************************************************************)

(** MapReduce jobs transform data using the MapReduce framework *)
module type WorkType = sig
  type input
  type output

  (** a unique identifier for the worktype *)
  val worktype_id   : id

  (** perform the work *)
  val run_work    : input -> output Deferred.t
end

(** The following three functions are used by the framework to find the module
    corresponding to a WorkType.  For each module W of type Work, you must call {[
      register_job (module J)
    ]}
    so that the framework can find the module when needed. *)

val load_worktype  : (module WorkType) -> unit
val get_worktype_for_id       : id           -> (module WorkType) option
val list_worktypes     : unit         -> id list
