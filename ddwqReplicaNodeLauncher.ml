open Async.Std
open Debug
open Work_full
open Protocol

(***********************************************************)
(* STATE VARS                                              *)
(***********************************************************)

type ip_address     = string
type listening_port = int

type master_connection = Async_extra.Import.Socket.Address.Inet.t * Async_extra.Import.Reader.t * Async_extra.Import.Writer.t

type node_id  = ip_address * listening_port
type node     = master_connection * node_id

type next_node    = node option
type prev_node    = node option 

type node_state = {
  master_ip   : string ref;
  master_port : int ref;
  chain_port  : int ref;
  user_port   : int ref;

  next_node : node option ref;
  prev_node : node option ref;

  id    : node_id ref;
  mconn : master_connection option ref;
  self  : node option ref;

}

let state = ref {
  master_ip   = ref "";
  master_port = ref (-1);
  chain_port  = ref (-1);
  user_port   = ref (-1);

  next_node = None;
  prev_node = None;

  id    = ref ("",-1);
  mconn = None;
  self  = None;
}


let should_terminate = Ivar.create()

(***********************************************************)
(* UTILITY FUNCTIONS                                       *)
(***********************************************************)

let node_id_to_string (nodeId : node_id) : string = 
  let (ip,port) = nodeId in
  "{Node@" ^ ip ^ (string_of_int port) ^ "}"


(***********************************************************)
(* REPLICA NODE FUNCTIONS                                  *)
(***********************************************************)


let begin_master_connection () = 
  debug INFO "Attempting to connect to Master-Service...");
  try_with ( fun () -> (Tcp.connect (Tcp.to_host_and_port ip_master port_master)) )
  >>= function
    | Core.Std.Result.Error e -> begin 
      debug ERROR "Failed to connect to Master-Service. Retry in 5 seconds";
      ((after (Core.Std.sec 5.0)) >>= fun _ -> begin_master_connection())
    end
    | Core.Std.Result.Ok m -> begin
      debug INFO "Connected to Master-Service";

      (* Finish initializing the state with info about the connection *)
      let (a,r,w) = m in
      !state.master_connection := Some((a,r,w));
      !state.self              := Some((a,r,w) , !state.!id);

      let module MSReq = MasterServiceRequest in
      let module MSRes = MasterServiceResponse in
      let module MSAck = MasterServiceAck in


      (* Get our initialization type from the master. This is either FirstChainMember or NewTail*)
      debug INFO "Sending our initialization request to the Master-Service with our ID";
      MSReq.send w (MSReq.InitRequest(!state.!id));


      debug INFO "Waiting for Master-Service resopnse to initialization request";
      MSRes.receive r
      >>= function
        | Core.Std.Result.Error e -> begin
          debug ERROR "Failed to receive initialization request from master. Resetting state and retrying in 5 seconds";
          !state.master_connection := None;
          !state.self              := None;
          ((after (Core.Std.sec 5.0)) >>= fun _ -> begin_master_connection())
        end
        
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
      +> flag "-port_chain" (required int)
         ~doc:"Replica chain communication port"
      +> flag "-port_user" (required int)
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

      begin_master_connection()

      >>| fun () -> (shutdown 0)
    )
  |> Command.run


