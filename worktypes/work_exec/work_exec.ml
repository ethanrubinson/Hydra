
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
    let offset = ref 0 in
    let rec find_open_dir () = 
      if not ((Sys.command ("mkdir slaveWork_" ^ string_of_int !offset)) = 0) then begin 
        offset := !offset + 1;
        find_open_dir ()
      end
      else begin 
        "slaveWork_" ^ string_of_int !offset ^ "/"
      end
    in
    let dir = find_open_dir () in


    Std.output_file ~filename:(dir ^ (fst input)) ~text:(snd input);
    ignore(Sys.command (("cs3110 compile " ^ dir ^ (fst input))));
    ignore(Sys.command (("cs3110 run " ^ dir ^ (fst input) ^ " > " ^ dir ^ "work.out")) );
    let chan = open_in (dir ^ "work.out") in
    let work_result = (input_all chan) in
    ignore(Sys.command (("rm " ^ dir ^ (fst input))));
    ignore(Sys.command ("rm " ^ dir ^ "work.out"));
    work_result


  let net_data_to_work_output net_data = net_data
end


(* Load the work-type *)
let () = Ddwq.load_worktype (module Work_Exec)