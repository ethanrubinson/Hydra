open Async.Std
open Work_full
let () =
  Command.async_basic
    ~summary:"Run the DDWQ Master Interface"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> flag "-addresses" (optional_with_default "addresses.txt" string)
         ~doc:"filename the file with the worker addresses"
      
    )
    (fun addresses () ->

      let split_host_and_port s =
        match Str.split (Str.regexp_string ":") s with
          | [host; port] -> (host, int_of_string port)
          | _            -> failwith "invalid host:port"
      in
      Reader.file_lines addresses >>= fun addresses ->
      print_endline "Loaded addresses:";
      List.iter print_endline addresses;
      let hps = List.map split_host_and_port addresses in
      
      let m = match (Ddwq.get_worktype_for_id "work.full") with |Some x -> x | None -> failwith "Worktype not loaded" in


      let module MyWork = (val m) in
      let module Master = DdwqController.Make(MyWork) in
      DdwqController.init hps;
      Master.run ()
      >>| fun () -> (shutdown 0)
    )
  |> Command.run


