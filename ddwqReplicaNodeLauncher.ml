open Async.Std
open Work_full
open Protocol
let () =
  Command.async_basic
    ~summary:"Run the DDWQ Replica Interface"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> flag "-addresses" (optional_with_default "addresses.txt" string)
         ~doc:"filename the file with the worker addresses"
      
    )
    (fun addresses () ->

     try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port "localhost" 33333)) )
      >>= function
        | Core.Std.Result.Error e -> (print_endline "[ERROR] Failed to connect to master service at ");
                       return ()
        | Core.Std.Result.Ok  v -> (print_endline "[INFO] Connected to master service at " );
                       let (_,r,w) = v in
                       let module MSReq = MasterServiceRequest in
                        let module MSRes = MasterServiceResponse in
                        let module MSAck = MasterServiceAck in

                         try_with (fun () -> return (MSReq.send w (MSReq.InitRequest(Unix.gethostname(),31103)  )))
                         >>= function
                             | Core.Std.Result.Error e -> (print_endline ("[ERROR] Failed " ^ ""));
                                            return ()
                             | Core.Std.Result.Ok    v' -> begin
                               (print_endline ("[INFO] Connected to mS, waiting for InitResponse " ^ ""));
                                
                                MSRes.receive r 
                                >>= function
                                | `Eof -> failwith "Shit"
                                | `Ok v -> begin
                                  match v with 
                                  |MSRes.FirstChainMember -> begin
                                    print_endline "I am the first in the chain";
                                    MSAck.send w MSAck.FirstChainMemberAck;
                                    never ()
                                  end
                                  |MSRes.NewTail -> begin 
                                    print_endline "I am a new tail";
                                    MSAck.send w MSAck.NewTailAck;
                                    never()
                                  end
                                  |_->failwith "No idea"
                                end

                             end 











      >>| fun () -> (shutdown 0)
    )
  |> Command.run


