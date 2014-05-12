open Async.Std

type id = string

type net_data = string

module type WorkType = sig
  type input
  type output

  (** a unique identifier for the worktype *)
  val worktype_id   : id

  val load_input_for_string   : string -> input
  val work_output_to_string   : output -> string
  val run_and_package_work    : input -> net_data
  val net_data_to_work_output : net_data -> output
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