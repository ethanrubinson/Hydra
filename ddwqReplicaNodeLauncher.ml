open Async.Std
open Debug
open Work_full
open Protocol
open Async_unix
(***********************************************************)
(* STATE VARS                                              *)
(***********************************************************)

type ip_address     = string
type listening_port = int
type node_id        = ip_address * listening_port

type ('a, 'b) t                 = ('a, 'b) Unix_syscalls.Socket.t
type ('a, 'b) master_connection = ([< `Active | `Bound | `Passive | `Unconnected ] as 'a, [< Socket.Address.t ] as 'b) t * Async_extra.Import.Reader.t * Async_extra.Import.Writer.t

type ('a, 'b) node      = ('a, 'b) master_connection * node_id
type ('a, 'b) next_node = ('a, 'b) node option
type ('a, 'b) prev_node = ('a, 'b) node option 

type ('a, 'b) node_state = {
  master_ip   : string ref;
  master_port : int ref;
  chain_port  : int ref;
  user_port   : int ref;

  next_node : ('a, 'b) node option ref;
  prev_node : ('a, 'b) node option ref;

  id    : node_id ref;
  mconn : ('a, 'b) master_connection option ref;
  self  : ('a, 'b) node option ref;

}

let state = ref {
  master_ip   = ref "";
  master_port = ref (-1);
  chain_port  = ref (-1);
  user_port   = ref (-1);

  next_node = ref None;
  prev_node = ref None;

  id    = ref ("",-1);
  mconn = ref None;
  self  = ref None;
}


(*let should_terminate = Ivar.create()*)

(***********************************************************)
(* UTILITY FUNCTIONS                                       *)
(***********************************************************)

let get_some (thing : 'a option) = match thing with | Some x -> x | _ -> failwith "Tried to get Some of None"

let node_id_to_string (nodeId : node_id) : string = 
  let (ip,port) = nodeId in
  "{Node@" ^ ip ^ (string_of_int port) ^ "}"



(***********************************************************)
(* REPLICA NODE FUNCTIONS                                  *)
(***********************************************************)

(*let rec when_should_terminate () = 
  (Ivar.read should_terminate) >>| fun _ -> ()*)


let close_socket_and_do_func m f = 
  (Socket.shutdown m `Both);
  !state.mconn := None;
  !state.self  := None;
  ((after (Core.Std.sec 5.0)) >>= fun _ -> f())

 

let rec begin_master_connection master_ip master_port () = 
  debug INFO "Attempting to connect to Master-Service...";
  try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port master_ip master_port)) )
  >>= function
    | Core.Std.Result.Error e -> begin 
      debug ERROR "Failed to connect to Master-Service. Retry in 5 seconds";
      ((after (Core.Std.sec 5.0)) >>= fun _ -> (begin_master_connection master_ip master_port ()))
    end
    | Core.Std.Result.Ok m -> begin
      debug INFO "Connected to Master-Service";

      (* Finish initializing the state with info about the connection *)
      let (a,r,w) = m in
      !state.mconn := Some((a,r,w));
      !state.self  := Some((a,r,w) , !state.!id);

      let module MSReq = MasterServiceRequest in
      let module MSRes = MasterServiceResponse in
      let module MSAck = MasterServiceAck in


      (* Get our initialization type from the master. This is either FirstChainMember or NewTail*)
      debug INFO "Sending our initialization request to the Master-Service with our ID";
      MSReq.send w (MSReq.InitRequest(!state.!id));


      debug INFO "Waiting for Master-Service resopnse to initialization request";
      MSRes.receive r
      >>= function
        | `Eof -> begin
          debug ERROR "Failed to receive initialization request from master. Resetting state and retrying in 5 seconds";
          close_socket_and_do_func a (begin_master_connection master_ip master_port)
        end
        
        | `Ok init_response -> begin
          match init_response with 
            | MSRes.FirstChainMember -> begin
              debug INFO "Initialization response received. We are the first chain member";
              debug INFO "Sending FirstChainMemberAck to Master-Service";
              MSAck.send w MSAck.FirstChainMemberAck;

              (*Once we have sent our response. We need to make sure that our request was not orphaned
                by another node requesting to be initialized and was also told they are the first in
                the chain by the master since our ACK may have been delayed...*)
              
              debug INFO "Waiting for either InitDone or InitFailed from Master-Service";
              MSRes.receive r
              >>= function
                 | `Eof -> begin
                    debug ERROR "Failed to receive InitDone or InitFailed. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                end
                | `Ok final_init_res -> begin 
                  match final_init_res with
                  | MSRes.InitDone -> begin
                    debug INFO "Our initialization request was successful. We are the first chain member";
                    
                    (* Ensure these are reset for sanity measures *)
                    !state.next_node := None;
                    !state.prev_node := None;

                    (* Okay we've connected now we can wait for user requests.... not sure how to do that just
                       yet so this comment will take its place for now*)
                    
                    never() (* We shall just literally do nothing but keep the socket alive for now*)

                  end
                  | MSRes.InitFailed -> begin
                    debug WARN "Our initialization request was orphaned. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                  end
                  | _ -> begin 
                    debug ERROR "Got unexpected response. Expected InitDone or InitFailed. Resetting state and retrying in 5 seconds";
                    close_socket_and_do_func a (begin_master_connection master_ip master_port)
                  end
                end

            end(*FirstChainMember Case*)
            | MSRes.NewTail -> begin
              debug INFO "Initialization response received. We are going to be a new tail node";
             


              MSAck.send w MSAck.NewTailAck;





              never() (* We shall just literally do nothing but keep the socket alive for now*)

              

            end (*NewTail Case*)
            | _ -> begin 
              debug ERROR "Received unexpected initialization response. Resetting state and retrying in 5 seconds";
              close_socket_and_do_func a (begin_master_connection master_ip master_port)
            end (*Unexpected reponse (wanted FirstChainMember or NewTail) Case*)
        end
    end




let () =
  Command.async_basic
    ~summary: "Run the DDWQ Replica Interface"
    ~readme: (fun () -> "This is a replica node of the DDWQ replication chain, the Master-Service must be running before it is launched.")
    Command.Spec.(
      empty
      +> flag "-masterip" (optional_with_default "localhost" string)
         ~doc:"IP/Hostname of Master-Service"
      +> flag "-masterport" (optional_with_default 33333 int)
         ~doc:"IP/Hostname of Master-Service"
      +> flag "-portchain" (required int)
         ~doc:"Replica chain communication port"
      +> flag "-portuser" (required int)
        ~doc:"Client interface communication port"
    )
    (fun ip_master port_master port_chain port_user () ->

      (Sys.command "clear")
      >>= fun _ ->

      debug NONE "########################";
      debug NONE "####  REPLICA NODE  ####";
      debug NONE "########################";
      debug NONE "";

      debug INFO ("Replica node started with Master-IP= " ^ ip_master ^ " Master-Port= " ^ string_of_int port_master ^ " Chain-Port= " ^ string_of_int port_chain ^ " User-Port= " ^ string_of_int port_user);

      (* Start initializing the state with cmdline args *)
      !state.master_ip   := ip_master;
      !state.master_port := port_master;
      !state.chain_port  := port_chain;
      !state.user_port   := port_user;
      !state.id          := ((Unix.gethostname()), port_chain);
      debug NONE ("Replica Node ID: " ^ node_id_to_string !state.!id);

      begin_master_connection ip_master port_master ()

      >>| fun () -> (shutdown 0)
    )
  |> Command.run


