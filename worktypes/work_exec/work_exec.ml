
open ExtLib

(******************************)
(**  Sys Command/Data  Work  **)
(******************************)

module Work_Exec = struct
  

  type filename = string
  type archive_data = string
  type exec_result = string


  type input = (filename * archive_data) 
  type output = (exec_result)

  let worktype_id = "work.exec"

  let load_input_for_string s = 
    let chan = open_in s in
    (s, input_all chan)


  let work_output_to_string o = o
  
  let run_and_package_work input = 
    Std.output_file ~filename:("slaveWork/" ^ (fst input)) ~text:(snd input);
    ignore(Sys.command (("cs3110 compile slaveWork/" ^ (fst input))));
    ignore(Sys.command (("cs3110 run slaveWork/" ^ (fst input) ^ " > slaveWork/work.out")) );
    let chan = open_in "slaveWork/work.out" in
    let work_result = (input_all chan) in
    ignore(Sys.command (("rm slaveWork/" ^ (fst input))));
    ignore(Sys.command ("rm slaveWork/work.out"));
    work_result


  let net_data_to_work_output net_data = net_data
end


(* Load the work-type *)
let () = Ddwq.load_worktype (module Work_Exec)