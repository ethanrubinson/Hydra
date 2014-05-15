(*
        ,--,                                                 
      ,--.'|                                                 
   ,--,  | :                   ,---,                         
,---.'|  : '                 ,---.'|   __  ,-.               
|   | : _' |                 |   | : ,' ,'/ /|               
:   : |.'  |       .--,      |   | | '  | |' |    ,--.--.    
|   ' '  ; :     /_ ./|    ,--.__| | |  |   ,'   /       \   
'   |  .'. |  , ' , ' :   /   ,'   | '  :  /    .--.  .-. |  
|   | :  | ' /___/ \: |  .   '  /  | |  | '      \__\/: . .  
'   : |  : ;  .  \  ' |  '   ; |:  | ;  : |      ," .--.; |  
|   | '  ,/    \  ;   :  |   | '/  ' |  , ;     /  /  ,.  |  
;   : ;--'      \  \  ;  |   :    :|  ---'     ;  :   .'   \ 
|   ,/           :  \  \  \   \  /             |  ,     .-./ 
'---'             \  ' ;   `----'               `--`---'     
                   `--`                                      

              - Developed by Ethan Rubinson. ethan@tebrotech.com "*)


open Async.Std
open Debug
open Protocol
open Socket
open UnixLabels

(***********************************************************)
(* STATE VARS                                              *)
(***********************************************************)

type ip_address     = string
type listening_port = int
type user_ports = (int * int) (*listening port * responding port)*)

type master_connection = Async_extra.Import.Socket.Address.Inet.t * Async_extra.Import.Reader.t * Async_extra.Import.Writer.t

type node_id  = ip_address * listening_port
type node     = master_connection * node_id * user_ports

type next_node    = node option
type prev_node    = node option 

type chain_table_entry  = prev_node * node * next_node
type chain_table        = (node_id , chain_table_entry) Hashtbl.t

let (chain : chain_table) = Hashtbl.create 10

type chain_state = {
  port        : int ref;
  client_port : int ref;

  head_node : node option ref;
  tail_node : node option ref;

  pending_new_tail  : bool ref;
  chain_size        : int ref;
}

let state = ref {
    port        = ref (-1);
    client_port = ref (-1);

    head_node = ref None;
    tail_node = ref None;

    pending_new_tail  = ref false;
    chain_size        = ref 0;
  }

let should_terminate = Ivar.create()
let state_mutex = Mutex.create()

let nework_state_string = ref "\027[0m\027[36m\027[1mIdle\027[0m\027[33m"

let screen_debug_msg = ref ""

(***********************************************************)
(* UTILITY FUNCTIONS                                       *)
(***********************************************************)
let pad s l = s ^ (String.make (l - String.length s) ' ')

let debug t string_to_print = (let t_String = "\027[1m" ^ (match t with |INFO -> "\027[1m\027[36m" |WARN -> "\027[1m\027[33m"|ERROR -> "\027[1m\027[31m"| FATAL -> "\027[31m\027[5m"|NONE -> "\027[1m\027[32m") in (screen_debug_msg := t_String ^ string_to_print); if String.length !screen_debug_msg > 50 then begin (screen_debug_msg := String.sub (!screen_debug_msg) 0 47); (screen_debug_msg := (!screen_debug_msg) ^ "...\027[0m") end  else (screen_debug_msg := (!screen_debug_msg) ^ "\027[0m"))


let is_none (thing : 'a option) = match thing with | None -> true | _ -> false

let get_some (thing : 'a option) = match thing with | Some x -> x | _ -> (debug FATAL "Tried to get Some of None."); failwith "Tried to get Some of None"

let get_node_liss_client_info (node : node) : (string * int) = 
  let (_,(ip,_),(lp,_)) = node in
  (ip,lp)

let get_node_resp_client_info (node : node) : (string * int) = 
  let (_,(ip,_),(_,rp)) = node in
  (ip,rp)


let get_node_writer (node : node) : Async_extra.Import.Writer.t = 
  let (mconn,_,_) = node in
  let (_,_,w) = mconn in
  w

let get_node_reader (node : node) : Async_extra.Import.Reader.t = 
  let (mconn,_,_) = node in
  let (_,r,_) = mconn in
  r


let get_node_id (node : node) : node_id = 
  let (_,node_id,_) = node in
  node_id

let node_id_to_string (nodeId : node_id) : string = 
  let (ip,port) = nodeId in
  "{Node@" ^ ip ^ (string_of_int port) ^ "}"

let node_to_string (node : node) : string = 
  let (_,node_id,_) = node in
  node_id_to_string node_id

let node_to_screen_print (node : node) : string = 
  let ((a,w,r),node_id,_) = node in
  (Address.to_string a)

let node_to_screen_print_chain_port (node : node) : string = 
  let (_,node_id,_) = node in
  let (_,port) = node_id in
  (string_of_int port)

let node_to_screen_print_listen_port (node : node) : string = 
  let (_,_,(liss,_)) = node in
  (string_of_int liss)

let node_to_screen_print_respond_port (node : node) : string = 
  let (_,_,(_,resp)) = node in
  (string_of_int resp)

let get_tail_node () : node = 
  (*(Mutex.lock state_mutex); *)
  let res = 
    match !state.!tail_node with 
    | Some x -> x 
    | None -> (debug FATAL "Requested a non-existant tail node."); failwith "State corrupted"
  in
  (*(Mutex.unlock state_mutex);*)
  res

let get_head_node () : node = 
  (*(Mutex.lock state_mutex); *)
  let res = 
    match !state.!head_node with 
    | Some x -> x 
    | None -> (debug FATAL "Requested a non-existant head node."); failwith "State corrupted"
  in
  (*(Mutex.unlock state_mutex);*)
  res


let nodes_are_equal (node1 : node) (node2:node) : bool = 
  let node1id = get_node_id node1 in
  let node2id = get_node_id node2 in
  if node1id = node2id then true else false


let chain_exists () : bool = 
  (*(Mutex.lock state_mutex);*)
  let exists = !state.!chain_size > 0 in
  (*(Mutex.unlock state_mutex);*)
  exists

let test_and_set_pending_tail () : bool = 
  (*(Mutex.lock state_mutex);*)
  let succ = (if !state.!pending_new_tail = false then begin
      !state.pending_new_tail := true;
      true
    end
     else begin 
       false
     end) in
  (*(Mutex.unlock state_mutex);*)
  succ

let eye = ref ""
let eye_list = ["⣾";"⣽";"⣻";"⢿";"⡿";"⣟";"⣯";"⣷"]
let eye_index = ref 0
let eye_length = List.length eye_list
let print_screen () = 
  don't_wait_for(Sys.command "clear"
       >>= fun _ ->
  (eye := List.nth eye_list (!eye_index));
  (eye_index := !eye_index + 1);

  if !eye_index = eye_length then
  (eye_index := 0);

  print_endline(
  "\027[0m\027[32m\027[1m                                                             .r                                             
                                           .                 2,     .                                        
                                          ;.                ,B     ,                                        
                                         .2                 @:    r,                                        
                                         A,     .          ;@    h:                                         
                                        :@    .;           @X   @r      .                                            ,--,                                                 
                                        @;   ,2           i@  :@#     r,                                          ,--.'|                                                 
                                       r@   r@            @@M#@@    s@r                                        ,--,  | :                   ,---,                         
                                       @#  3@      ;     A@@@@@@  2@@,                                      ,---.'|  : '                 ,---.'|   __  ,-.               
                                      2@@r@@2    GM.    .@@@@@@@@@@@                                        |   | : _' |                 |   | : ,' ,'/ /|               
                                      @@@@@@r .&@@      @@@@@@@@@@@@ ,r,                                    :   : |.'  |       .--,      |   | | '  | |' |    ,--.--.    
                                     #@@@@@@@@@@#     X@@@@@@@@@@@@; ,.                                     |   ' '  ; :     /_ ./|    ,--.__| | |  |   ,'   /       \\   
                                    :@@@@@@@@@@@; :, @@@@@@@@@@@@@9 ;5    .                                 '   |  .'. |  , ' , ' :   /   ,'   | '  :  /    .--.  .-. |  
                                   #@@@@@@@@@@@@@2, #@@@@@@@@@@@@@.r     .,                                 |   | :  | ' /___/ \\: |  .   '  /  | |  | '      \\__\\/: . .  
                                 :@@@@@@@@@@@@@@@5  @@\027[0m\027[31m"^(!eye)^"\027[0m\027[32m\027[1m@@@@@@@@@@.ir\027[0m\027[32m@M\027[0m\027[32m\027[1m  ;.                                  '   : |  : ;  .  \\  ' |  '   ; |:  | ;  : |      ,\" .--.; |  
                                .@@@@@@@@@@@@@@:   ,@G;@@@@@@@@&  A\027[0m\027[32mS@@X\027[0m\027[32m\027[1mr.     .                             |   | '  ,/    \\  ;   :  |   | '/  ' |  , ;     /  /  ,.  |  
                                @@\027[0m\027[31m"^(!eye)^"\027[0m\027[32m\027[1m@@@@@@@@@@&.    ,@@@@@@@@@A  .@\027[0m\027[32m:@@;\027[0m\027[32m\027[1mGr     :;                             ;   : ;--'      \\  \\  ;  |   :    :|  ---'     ;  :   .'   \\ 
                                @s;@@@@@@@@@:\027[0m\027[32m,@@@.\027[0m\027[32m\027[1m @@@@@@@@A    s@ \027[0m\027[32m2,\027[0m\027[32m\027[1m@S\027[0m\027[32m@&\027[0m\027[32m\027[1m   ;,                              |   ,/           :  \\  \\  \\   \\  /             |  ,     .-./ 
                                @@@@@@@@@@s\027[0m\027[32m,#@@@@;\027[0m\027[32m\027[1m#@@@@@@h      @@\027[0m\027[32mri\027[0m\027[32m\027[1m@@.\027[0m\027[32m@H\027[0m\027[32m\027[1m ;@r                               '---'             \\  ' ;   `----'               `--`---'     
                               ;@@@@@@@@i   \027[0m\027[32m,M@@.\027[0m\027[32m\027[1m@@@@@@2       M@@@@@# \027[0m\027[32mSS\027[0m\027[32m\027[1m@@,   .                                                `--`                                      
                              r@@@@@@@S       \027[0m\027[32m@@\027[0m\027[32m\027[1m 2ii2s        ,@@@@@@@@@@@   .;                               
                             G@@@@@@2         \027[0m\027[32m@@@#B,\027[0m\027[32m\027[1m          @@@@@@@@@@@@ ::.                             \027[0m\027[33m,---------------------------------------------------------,==.
                             \027[0m\027[32m\027[1m@#HHM&           \027[0m\027[32m@@@@@:\027[0m\027[32m\027[1m        5@@@@@@@@@@@@@@;                              \027[0m\027[33m/                                                         /__  \\
                             \027[0m\027[32m\027[1m@#2@i           \027[0m\027[32mM@@@@@      5\027[0m\027[32m\027[1m;@@@@@@@@@@@@@@@#:                              \027[0m\027[33m\\        | \027[0m\027[33m\027[1mMaster-Service Configuration Details\027[0m\027[33m |         |(_\\ /
                                            \027[0m\027[32m;@@@@@     .@M\027[0m\027[32m\027[1mA@@@@@@@@@@@@@2:\027[0m\027[32m                                \027[0m\027[33m/  --- . --- . --- . --- . --- . --- . --- . --- . --- .  \\-`-'
                                            \027[0m\027[32m@@@@@:    s@@\027[0m\027[32m\027[1m.@\027[0m\027[31m"^(!eye)^"\027[0m\027[32m\027[1mX@@@@@@@@@@:\027[0m\027[32m.@&                               \027[0m\027[33m>                                                         /
                                           \027[0m\027[32m@@@@@A    H@@M\027[0m\027[32m\027[1m,@;@@@@@@@@@S.\027[0m\027[32m5@@@;                              \027[0m\027[33m} » IP   : \027[0m\027[33m\027[1m" ^ (pad (UnixLabels.string_of_inet_addr (List.hd (Array.to_list ((UnixLabels.gethostbyname (UnixLabels.gethostname())).h_addr_list)))) 47) ^ "\027[0m\027[33m}
                                          \027[0m\027[32mH@@@@@    @@@@G\027[0m\027[32m\027[1m.@@@@@@@@@H,\027[0m\027[32mi@@@@@@                              \027[0m\027[33m| » Ports → Chain: \027[0m\027[33m\027[1m"^ (pad (string_of_int (!state.!port)) 5)^"\027[0m\027[33m" ^ " | Client: \027[0m\027[33m\027[1m"^ (pad (string_of_int (!state.!client_port)) 23)^"\027[0m\027[33m" ^ "\\
                                          \027[0m\027[32m@@@@@@   @@@@@\027[0m\027[32m\027[1m M@@@@@@@B\027[0m\027[32m    H@@@@@                              \027[0m\027[33m\\                                                         /
                                         \027[0m\027[32mi@@@@@@, @@@@@\027[0m\027[32m\027[1m G@@@@@@A\027[0m\027[32m      S@@@@@                              \027[0m\027[33m}                                                         {
                                         \027[0m\027[32mA@@@@@@rS@@@#\027[0m\027[32m\027[1m #@@@@@X\027[0m\027[32m       H@@@@@S                              \027[0m\027[33m\\                 | \027[0m\027[33m\027[1mChain Information\027[0m\027[33m |                   /
                         \027[0m\027[32m:;:.            \027[0m\027[32mA@@@@@@ @@@@X\027[0m\027[32m\027[1m 923A5\027[0m\027[32m     .5@@@@@@@B                               \027[0m\027[33m}  --- . --- . --- . --- . --- . --- . --- . --- . --- .  >
                        \027[0m\027[32m,r5@@@           2@@@@@,X@@@@@@#H;  r:2@@@@@@@@@@r  .r9#@@@@@@@@@Ai:              \027[0m\027[33m>                                                         /
                      \027[0m\027[32m.5A@@@@@           ,@@@@@ @@@@@@@A;i#@@@@@@@@@@@@@3r@@@@@@@@@@@@@@@@@@@@A:          \027[0m\027[33m} » Head Node ID   : \027[0m\027[33m\027[1m" ^ pad (if is_none !state.!head_node then "N/A" else node_to_screen_print (get_head_node())) 37^ "\027[0m\027[33m}
                          \027[0m\027[32m#@@@&           @@@@i:@@@@@@.;@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2        \027[0m\027[33m|     Chain Port   : \027[0m\027[33m\027[1m"^ pad (if is_none !state.!head_node then "N/A" else node_to_screen_print_chain_port (get_head_node())) 37^ "\027[0m\027[33m\\
                           \027[0m\027[32m@@@@          ,X@@@.9@@@@A G@@@@@@@@@@@@@@@@@@@@@@@#Sr;;r2@@@@@@@@@@@@@@:      \027[0m\027[33m\\     Listen Port  : \027[0m\027[33m\027[1m"^ pad (if is_none !state.!head_node then "N/A" else node_to_screen_print_listen_port (get_head_node())) 37^ "\027[0m\027[33m/
                         \027[0m\027[32m:,M@@@,,      2@@ @@@ #@@@A @@@@@#2S2H@@@@@@@@@@@@@@:.2M##As.;@@@@@@@@@@@@@.     \027[0m\027[33m}                                                         {
                          \027[0m\027[32m,riGB@@@#&B@@@@@s:@@ @@@# @@@@@s S3i: #@@@@@@@@@@@::@@@@@@@@ ,5.S@@@@@@@@@@     \027[0m\027[33m\\ » Tail Node ID   : \027[0m\027[33m\027[1m"^ pad (if is_none !state.!tail_node then "N/A" else node_to_screen_print (get_tail_node())) 37^ "\027[0m\027[33m/
                              \027[0m\027[32ms@@@@@@@@@@@@;i@.#@@ &@@@@A @@@@@5 @@@@@@@@@@@ @@@@@@@@@@2A &@@@@@@@@@@G    \027[0m\027[33m}     Chain Port   : \027[0m\027[33m\027[1m"^ pad (if is_none !state.!tail_node then "N/A" else node_to_screen_print_chain_port (get_tail_node())) 37^ "\027[0m\027[33m>
                                  \027[0m\027[32mrM@@M3;    iS2@S:@@@@@ 3@@@@@@ @@@@@@@@@@@ @@@@@@@@@@@;,@@@@@@@@@@@@    \027[0m\027[33m}     Respond Port : \027[0m\027[33m\027[1m"^ pad (if is_none !state.!tail_node then "N/A" else node_to_screen_print_respond_port (get_tail_node())) 37^ "\027[0m\027[33m}
                                                \027[0m\027[32m@.@@@@@2.@@@@@@A 2h@@@@@@@@@ H@@@@@@@@@@@ @@@@@@@@@@@@    \027[0m\027[33m>                                                         /
                   \027[0m\027[32m,:  r&@@@@@@@@@@@@@@Gr.       ;@@@@h @@@@@@@@. i@@@@@@@@@h @@@@@@@@@@:  ;#@@@@@@@@@    \027[0m\027[33m|                                                         \\
               \027[0m\027[32mA@@@@2A@@@B3Sii5hM@@@@@@@@@@@9:     r#5 @@@@@@@r,A@@@@@@@@@@@@r;@@@@@@@@     5@@@@@@@@S    \027[0m\027[33m\\                   | \027[0m\027[33m\027[1mNetwork Status\027[0m\027[33m |                    /
           \027[0m\027[32m,s#S #@@r:B@             .2@@@@@@@@@@r    i@@@@@@2;M@@@@@@@@@@@@Mi  @@@@@@H    .@@@@@@@@@B     \027[0m\027[33m}  --- . --- . --- . --- . --- . --- . --- . --- . --- .  {
     \027[0m\027[32m:r9@@@@@h.@@@@@@@@                 ,h@@@@@@@@@X,ss3#@2  ;X2X92ssr;:,,    #@@@@@;   s@@@@@@@@@@S      \027[0m\027[33m\\                                                         /
    \027[0m\027[32m:#@@@@@@ ;@@@9iS@@                      rM@@@@@@@@9;:.         9@@@@@    #@@#&2. r@@@@@@@@@@@A        \027[0m\027[33m} » Chain Length  : \027[0m\027[33m\027[1m"^ pad (string_of_int (max 0 (!state.!chain_size))) 38^ "\027[0m\027[33m>
      \027[0m\027[32m@@@@@  @;   i  ,                         .i&@@@@@@@@@@#2;.  :h3399    :Gr:;rX@@@@@@@@@@@@X          \027[0m\027[33m| » Network State : " ^ pad (if !state.!chain_size > 0 then "\027[0m\027[32m\027[1mActive\027[0m\027[33m" else begin if !state.!chain_size = 0 then "\027[0m\027[36m\027[1mIdle\027[0m\027[33m" else "\027[0m\027[31m\027[1mRestart Needed.\027[0m\027[33m" end) 60^"{____
     \027[0m\027[32m;@@@@@:5iX@@@r                         X#@@#2rr2B@@@@@@@@@@@@@#A9hA@@@@@@@@@@@@@@@@@@@@G,            \027[0m\027[33m}                 ↪ \027[0m"^ pad !screen_debug_msg 55 ^ "\027[0m\027[33m|__( \\
     \027[0m\027[32m@@@@@@@@@@@@s                        :@@@@@@@@Bi,  ,iA@@@@@@@@@@@@@@@@@@@@@@@@@@@@@h;                \027[0m\027[33m\\                                                         \\    /
    \027[0m\027[32m@@@@@@@H&@@@s                         3 ;@;             .S5:,;i3AM@@@###@@@MAXi;                       \027[0m\027[33m`---------------------------------------------------------`=='  
   \027[0m\027[32mM@@3:      @r                            ;.               i          ,39i:                               
   \027[0m\027[32mr.                                                                    r,                                 \027[0m"); return ())


(***********************************************************)
(* MASTER-SERVICE FUNCTIONS                                *)
(***********************************************************)

let rec when_should_terminate () = 
  (Ivar.read should_terminate) >>| fun _ -> ()


let print_chain_structure () = 
  (*(Mutex.lock state_mutex);*)
  let output_string = ref "\n" in
  (if !state.!chain_size = 0 then begin
      output_string := "No chain"
    end
   else begin
     let head_key = get_node_id (get_head_node()) in
     let rec print_structure key = 
       let found_entry = Hashtbl.find chain key in
       let (_, this, next) = found_entry in
       (output_string := !output_string ^ "\t" ^ (node_to_string this) ^ " -->\n");
       if not (is_none next) then begin
         let next_node = get_some next in
         print_structure (get_node_id next_node)
       end
       else begin
         ()
       end
     in
     print_structure head_key
   end);
  debug INFO ("Chain Structure :" ^ !output_string)
(*(Mutex.unlock state_mutex)*)


let restructure_chain_for_failed_node (node:node) = 
  (*First see where in the chain this node is*)
  (*(Mutex.lock state_mutex);*)
  let curr_head = get_head_node() in
  let curr_tail = get_tail_node() in
  if nodes_are_equal node curr_head then begin
    (*The head has failed*)
    (*debug WARN (node_to_string node ^ " Was the head node. Assigning new head.");*)
    debug WARN ("The head node has died. Reassigning.");
    (*Find the next node in the chain (the one after the head)*)
    let dead_head_table_entry = Hashtbl.find chain (get_node_id curr_head) in
    let (_, dead_head_node, dead_head_next) = dead_head_table_entry in

    (* Sanity check to make sure that we have more nodes in the chain. If we don't we have a serious problem *)
    if (is_none dead_head_next) then begin
      (* ..... We're fucked *)
      debug FATAL ("All Replica nodes have failed");
      (Ivar.fill_if_empty should_terminate "All replicas have failed");
      !state.chain_size := (-1);
      !state.head_node := None;
      !state.tail_node := None;
      (*(Mutex.unlock state_mutex);*)
      (*return()*) (*Returning should get rid of the socket connection dispatched to the failed node*)
      return ()
    end
    else begin
      (*This is the exepcted scenario, message the next node in the chain to alter it of its new position*)
      let new_head_node = get_some dead_head_next in
      let module MSMonitor = MasterMonitorComm in
      (MSMonitor.send (get_node_writer new_head_node) MSMonitor.YouAreNewHead);

      !state.chain_size := !state.!chain_size - 1;
      !state.head_node := Some(new_head_node);
      let (_,new_head,new_head_next) = Hashtbl.find chain (get_node_id new_head_node) in
      Hashtbl.replace chain (get_node_id new_head_node) (None,new_head,new_head_next);
      Hashtbl.remove chain (get_node_id node);


      (*(Mutex.unlock state_mutex);*)
      return()
    end
  end (*case of head*)

  else begin
    if nodes_are_equal node curr_tail then begin
      (*The tail has failed*)
      (*debug WARN (node_to_string node ^ " Was the tail node. Assigning a new tail.");*)
      debug WARN ("The tail node has died. Reassigning.");
      (*Find the prev node in the chain (the one before the tail)*)
      let dead_tail_table_entry = Hashtbl.find chain (get_node_id curr_tail) in
      let (dead_tail_prev, dead_tail_node, _) = dead_tail_table_entry in

      (* Sanity check to make sure that we have more nodes in the chain. If we don't we have a serious problem *)
      if (is_none dead_tail_prev) then begin
        debug FATAL ("All Replica nodes have failed");
        (Ivar.fill_if_empty should_terminate "All replicas have failed");
        !state.chain_size := (-1);

        (*(Mutex.unlock state_mutex);*)
        (*return()*) (*Returning should get rid of the socket connection dispatched to the failed node*)
        return ()
      end
      else begin
        (*This is the exepcted scenario, message the next node in the chain to alter it of its new position*)
        let new_tail_node = get_some dead_tail_prev in
        let module MSMonitor = MasterMonitorComm in
        (MSMonitor.send (get_node_writer new_tail_node) MSMonitor.YouAreNewTail);
        !state.chain_size := !state.!chain_size - 1;
        !state.tail_node := Some(new_tail_node);
        let (new_tail_prev,new_tail,_) = Hashtbl.find chain (get_node_id new_tail_node) in
        Hashtbl.replace chain (get_node_id new_tail_node) (new_tail_prev,new_tail,None);
        Hashtbl.remove chain (get_node_id node);

        (*(Mutex.unlock state_mutex);*)
        return()
      end
    end (*case of tail tail node*)

    else begin
      (*No need to check if prev/succ exists since this is a middle node*)
      (*debug WARN (node_to_string node ^ " Was a middle node. Cutting it out of the chain");*)
      debug WARN ("A middle node died. Re-linking chain.");
      (*Find the prev node in the chain (the one before the tail)*)
      let dead_mid_table_entry = Hashtbl.find chain (get_node_id node) in
      let (dead_mid_prev, dead_mid_node, dead_mid_next) = dead_mid_table_entry in
      let prev_node = get_some dead_mid_prev in
      let next_node = get_some dead_mid_next in
      let module MSMonitor = MasterMonitorComm in

      (*Send message to the prev node indicating that it has a new next node*)
      (MSMonitor.send (get_node_writer prev_node) (MSMonitor.YouHaveNewNextNode(get_node_id next_node)));

      (MSMonitor.send (get_node_writer next_node) (MSMonitor.YouHaveNewPrevNode((get_node_id prev_node),-1)));
      !state.chain_size := !state.!chain_size - 1;
      let (p',_,_) = Hashtbl.find chain (get_node_id prev_node) in
      Hashtbl.replace chain (get_node_id prev_node) (p',prev_node,dead_mid_next);

      let (_,_,n') = Hashtbl.find chain (get_node_id next_node) in
      Hashtbl.replace chain (get_node_id next_node) (dead_mid_prev,next_node,n');

      Hashtbl.remove chain (get_node_id node);

      return()


    end (*case of middle*)
  end (*case of not head*)



let monitor_node (node : node) = 
  let module MSHeartbeat = MasterMonitorComm in
  let r = get_node_reader node in
  let rec do_monitor () = 
    MSHeartbeat.receive r
    >>= function
    | `Eof -> begin
        (*debug WARN (node_to_string node ^ " Has died.");*)
        restructure_chain_for_failed_node node 
      end
    | `Ok _ -> begin (*This is pretty useless. Since nodes are fail-stop `Eof will always trigger*)
        do_monitor()
      end
  in
  do_monitor()



let begin_chain_listening_service_on_port p = 

  let module MSReq = MasterServiceRequest in
  let module MSRes = MasterServiceResponse in
  let module MSAck = MasterServiceAck in
  let module MSMonitor = MasterMonitorComm in

  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port p)
    (fun addr r w  ->
       let mconnection = (addr,r,w) in
       (*let addr_string = Address.to_string addr in*)
       debug INFO ("Incoming chain connection request.");
       debug INFO ("Waiting for chain init request.");
       MSReq.receive r
       >>= function
       | `Eof -> begin 
           debug WARN ("Socket was closed by chain node.");
           return ()
         end
       | `Ok msg -> begin 
           match msg with
           | MSReq.InitRequest((ip,port,(usr_liss,usr_resp))) -> begin 
               let connected_node_id = (ip,port) in
               debug INFO ("Chain init request received.");

               if not (chain_exists()) then begin
                 (* Since there are no head nodes. Send a request indicating the start of a new replica chain *)
                 debug INFO ("Sending FirstChainMember response.");
                 (MSRes.send w MSRes.FirstChainMember);

                 (* Wait for ACK *)
                 MSAck.receive r 
                 >>= function 
                 | `Eof -> begin
                     debug ERROR ("Failed to receive FCM ACK.");
                     return ()
                   end
                 | `Ok MSAck.FirstChainMemberAck -> begin
                     debug INFO ("Got FCM ACK. Initializing chain.");
                     (* Perform sanity check to ensure another request did not come in while we were waiting for this ACK*)
                     if not (chain_exists()) then begin

                       (*(Mutex.lock state_mutex);*)
                       let new_chain_node = (mconnection, connected_node_id,(usr_liss,usr_resp)) in
                       !state.head_node := Some(new_chain_node);
                       !state.tail_node := Some(new_chain_node);
                       !state.chain_size := !state.!chain_size + 1;
                       Hashtbl.add chain connected_node_id (None,new_chain_node,None);
                       debug NONE ("Chain initialized.");
                       (MSRes.send w MSRes.InitDone);
                       (*(Mutex.unlock state_mutex);*)

                       monitor_node new_chain_node

                     end
                     else begin
                       (* Strangely enough, the initial request was orphaned by another node initialization *)
                       debug WARN ("Initialization as FCM was orphaned.");
                       (MSRes.send w MSRes.InitFailed);
                       return ()
                     end
                   end
                 | _ -> begin
                     debug ERROR ("Got unexpected res from chain node.");
                     return ()
                   end
               end (* Case of new chain *)


               else begin

                 (* Check to ensure that there is not currently a new pending tail node. This would royally mess up our state *)
                 if (test_and_set_pending_tail() = false) then begin 
                   debug WARN ("New tail node is already pending.");
                   (MSRes.send w MSRes.InitFailed);
                   return ()
                 end (* Case of pending tail *)

                 else begin
                   (* There is allready an established chain. Initialize this new node as the new tail *)
                   debug INFO ("Sending NewTail response.");

                   (* Alert the current tail that it is about to lose its job. When the NewTail ACK is received. 
                      it is assumed that the communication between the current tail and the new tail is completed *)

                   let current_tail = get_tail_node() in
                   let tail_id = get_node_id current_tail in

                   (MSRes.send w MSRes.NewTail);
                   (MSMonitor.send (get_node_writer current_tail) (MSMonitor.PrepareNewTail(ip,port)));

                   (* Wait for ACK *)
                   MSAck.receive r 
                   >>= function 
                   | `Eof -> begin
                       debug ERROR ("Failed to receive NT ACK.");
                       (*(Mutex.lock state_mutex);*)
                       (!state.pending_new_tail := false);
                       (*(Mutex.unlock state_mutex);*)

                       return ()
                     end
                   | `Ok MSAck.NewTailAck -> begin

                       (* Restructure the chain *)

                       (*(Mutex.lock state_mutex);*)
                       let new_chain_node = (mconnection,connected_node_id,(usr_liss,usr_resp)) in
                       let new_curr_tail_next = Some(new_chain_node) in
                       let (tail_prev,the_tail,_) = Hashtbl.find chain tail_id in
                       Hashtbl.replace chain tail_id (tail_prev,the_tail,new_curr_tail_next);
                       Hashtbl.add chain connected_node_id (Some(the_tail),new_chain_node,None);
                       !state.tail_node := Some(new_chain_node);
                       !state.pending_new_tail := false;
                       !state.chain_size := !state.!chain_size + 1;


                       debug NONE ("A new tail was initialized.");

                       (*This has been taken out for the momment. It is unecessary and may cause problems*)
                       (*(MSRes.send w MSRes.InitDone);*)


                       (*(Mutex.unlock state_mutex);*)

                       monitor_node new_chain_node

                     end
                   | _ -> begin
                       debug ERROR ("Got unexpected res from chain node.");
                       return ()
                     end

                 end (* Case of no pending tail *)

               end (* Case of existing chain *)


             end (* Match InitReq *)


         end

    )
  >>= fun server ->
  debug INFO "Hyrda Started. Waiting on first node.";
  when_should_terminate()
  >>= fun _ ->
  (Tcp.Server.close server)
  >>= fun _ -> 
  return 0



let begin_client_listening_service_on_port p = 

  let module ClReq = ClientToMasterRequest in
  let module ClRes = ClientToMasterResponse in

  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port p)
    (fun addr r w  ->
       debug INFO ("Incoming client connection request.");
       debug INFO ("Waiting for client init request.");
       ClReq.receive r
       >>= function
       | `Eof -> begin 
           debug WARN ("Socket was closed by client.");
           return ()
         end
       | `Ok msg -> begin 
           match msg with
           | ClReq.HeadAndTailRequest -> begin 
              debug INFO ("Client init request received.");
              if not (chain_exists()) then begin
                debug INFO ("No chain. Sending client None");
                (ClRes.send w (ClRes.HeadAndTailResponse(None)));
                return()
              end
              else begin
                debug INFO ("Sending client the Head & Tail info.");
                (ClRes.send w (ClRes.HeadAndTailResponse(Some(get_node_liss_client_info (get_head_node()), get_node_resp_client_info (get_tail_node())))));
                return()
              end (* Case of existing chain *)
            end (* Match InitReq *)

         end

    )
  >>= fun server ->
  when_should_terminate()
  >>= fun _ ->
  (Tcp.Server.close server)
  >>= fun _ -> 
  return 0



(***********************************************************)
(* MAIN                                                    *)
(***********************************************************)
let () =

  Command.async_basic
    ~summary: "Run Hydra's 'Master-Service'"
    ~readme: (fun () -> "This is the Master-Service for the DDWQ replication chain.")
    Command.Spec.(
      empty
      +> flag "--config" (optional_with_default "DDWQ.cfg" string)
        ~doc:"Path to configuration file"
    )
    (fun config () ->

       (Sys.command "clear")
       >>= fun _ ->

       let get_port_number_from_string line =
         try ( let x = int_of_string line in if x < 1024 || x > 49151 then raise (Failure "") else x ) with 
         | Failure e -> begin
             debug FATAL "\"port\" must be within [1024-49151].";
             failwith "Invalid port format"
           end
       in
       let process_config_file_line s =
         match Str.split (Str.regexp_string ":") s with
         | [prefix; value] -> begin
             if prefix = "chainport" then begin
               (*(Mutex.lock state_mutex);*)
               (!state.port := (get_port_number_from_string value));
               (*(Mutex.unlock state_mutex)*)
             end
             else begin
                if prefix = "clientport" then begin
                 (*(Mutex.lock state_mutex);*)
                 (!state.client_port := (get_port_number_from_string value));
                 (*(Mutex.unlock state_mutex)*)
                end
                else begin
                  debug FATAL ("Invalid configuration prefix.");
                  failwith "Configuration file not formatted propperly"  
                end
               
             end
           end
         | _               -> begin
             debug FATAL ("Could not parse config line.");
             failwith "Failed parsing configuration file"
           end
       in

       debug INFO "Loading configuration file.";
       try_with (fun () -> Reader.file_lines config) 
       >>= function
       |Core.Std.Result.Error e -> begin
           debug FATAL ("Could not open config file.");
           failwith "Could not open config file"
         end
       |Core.Std.Result.Ok config_lines -> begin
           (if List.length config_lines == 0 then begin
               debug FATAL "Configuration file empty.";
               failwith "Configuration file empty"
             end
            else begin
              (List.iter process_config_file_line config_lines);
              debug INFO "Config file loaded.";
              (*(Mutex.lock state_mutex);*)
              let port_num = !state.!port in
              let client_port_num = !state.!client_port in
              (*(Mutex.unlock state_mutex);*)
              debug NONE ("Starting Hydra on port: " ^ (string_of_int port_num));



              ignore(every ~stop:((*when_should_terminate()*) never()) (Core.Std.sec 0.2) (
                  fun () -> (*print_chain_structure()*) print_screen()
                ));



              (* Begin *)
              Deferred.all [
                (*((after (Core.Std.sec 150.0)) >>= fun _ -> (Ivar.fill_if_empty should_terminate "Timed out"); (return 0)) ;*)

                (begin_chain_listening_service_on_port port_num) ;
                (begin_client_listening_service_on_port client_port_num);
                ((when_should_terminate()) >>= fun _ -> return 0) ;
                never()
              ] >>= fun  x -> return x 

            end);
         end
         >>= fun exit_codes -> (*(print_int_list exit_codes);*) (after (Core.Std.sec 5.0)) >>| fun _ -> (Async.Std.shutdown (List.hd exit_codes))


    )
  |> Command.run

