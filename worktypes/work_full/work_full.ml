open Async.Std
open Async_unix

(******************************)
(**  Sys Command/Data  Work  **)
(******************************)

module Work_Full = struct
  (*type sys_command = string
  type archive_name = string
  type archive_data = string

  type input = (sys_command * archive_name * archive_data) 
  type output = (archive_name * archive_data)

  let worktype_id = "work.full"

  let run_work input = failwith "run_work for type work.full not yet implemented"*)

  type input = (string) 
  type output = (string)

  let worktype_id = "work.full"

  let load_input_for_string s = (s ^ "-loaded")
  let work_output_to_string o = (String.sub o 0 ((String.length o) - (String.length "-loaded")))
  
  let run_and_package_work input = (input ^ "-result")
  let net_data_to_work_output net_data = (String.sub net_data 0 ((String.length net_data) - (String.length "-result")))
end


(* Load the work-type *)
let () = Ddwq.load_worktype (module Work_Full)