open Async.Std
open Async_unix

(******************************)
(**  Simple Adder            **)
(******************************)

module Work_Addi = struct

  type input = (int * int) 
  type output = (int)

  let worktype_id = "work.addi"

  let load_input_for_string s = match Str.split (Str.regexp_string " ") s with | [f;s] -> (int_of_string f, int_of_string s) | _ -> failwith "Invalid input. Must be formatted \"n1 n2\""
  let work_output_to_string o = string_of_int o
  
  let run_and_package_work input = string_of_int ( (fst input) + (snd input) )
  let net_data_to_work_output net_data = int_of_string net_data
end


(* Load the work-type *)
let () = Ddwq.load_worktype (module Work_Addi)