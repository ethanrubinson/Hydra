open Async.Std

type id = string

module type WorkType = sig
  type input
  type output

  (** a unique identifier for the worktype *)
  val worktype_id   : id

  (** perform the work *)
  val run_work    : input -> output Deferred.t
end

let worktype_table = Hashtbl.create 8

let load_worktype work =
  let (module W : WorkType) = work in
  if Hashtbl.mem worktype_table W.worktype_id
    then failwith ("duplicate work registration: " ^ W.worktype_id)
    else Hashtbl.add worktype_table W.worktype_id work

let get_worktype_for_id worktype_id =
  if Hashtbl.mem worktype_table worktype_id
    then Some (Hashtbl.find worktype_table worktype_id)
    else None

let list_worktypes () =
  Hashtbl.fold (fun k _ a -> k::a) worktype_table []