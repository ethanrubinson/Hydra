open Async.Std
open Ddwq
open Protocol
open Debug


let () =
  Command.async_basic
    ~summary:"Run the DDWQ Client Interface"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> flag "-ip"(required string)
         ~doc:"IP/Hostname of head in the chain"
      +> flag "-port"(required int)
         ~doc:"Listening port of head in the chain"
      
    )
    (fun ip port () ->
      
      let m = match (Ddwq.get_worktype_for_id "work.full") with |Some x -> x | None -> failwith "Worktype not loaded" in
      let module MyWork = (val m) in 
      ignore(every ~stop:(never()) (Core.Std.sec 1.0) (
              fun () -> don't_wait_for(

                try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip port)) )
                >>= function
                | Core.Std.Result.Error e -> begin 
                  debug ERROR ("Failed to connect to chain");
                  return()
                end
                | Core.Std.Result.Ok m -> begin

                  (* Finish initializing the state with info about the connection *)
                  let (a,r,w) = m in
                  (*!state.next_node := Some((a,r,w),(ip,port));*) (*Change... see below*)
                  let module CIReq = ClientInitRequest in
                  let module CIRes = ClientInitResponse in
                  let module CReq = ClientRequest in
                  let module CRes = ClientResponse in

                  debug INFO "Connected to the chain.";


                  debug INFO "Sending worktype_id.";

                  CIReq.send w (CIReq.InitForWorkType(MyWork.worktype_id));


                  debug INFO "Waiting on response";
                  CIRes.receive r
                  >>= function
                    | `Eof  -> begin 
                      debug INFO "Got an error while waiting for response!";
                      return()
                    end
                    | `Ok msg -> begin 
                      match msg with
                        | CIRes.InitForWorkTypeSucceeded -> begin 
                          debug INFO "SUCCESS!";
                          return()
                        end 
                        | CIRes.InitForWorkTypeFailed f -> begin 
                          debug INFO f;
                          return()
                        end
                    end

                end


              )
            ));





      never()

      >>| fun () -> (shutdown 0)
    )
  |> Command.run